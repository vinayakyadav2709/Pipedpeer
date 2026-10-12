package nixstore

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// writeBundle generates the OCI bundle that runs a nix command with the
// private store bound at /nix.
//
// This is deliberately the smallest namespace that does the job: user (to get
// the privilege needed to mount at all) and mount (to put the store at /nix).
// No pid, ipc, uts, cgroup or network namespace, because none of them are
// wanted here - the code being run is our own nix, not a user's job, and
// isolating it buys nothing while costing compatibility. Dropping uts in
// particular avoids a sethostname that fails inside an unprivileged nested
// container, and keeping the host network is required: nix has to reach
// cache.nixos.org.
//
// Everything on the host root is bound through read-only, except the user's
// home (nix writes its cache and profile there) and the store itself.
func writeBundle(dir string, args []string, cwd string) error {
	rootfs := filepath.Join(dir, "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		return err
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	if cwd == "" {
		var err error
		if cwd, err = os.Getwd(); err != nil {
			cwd = home
		}
	}

	mounts := []mount{
		// The whole point: the private store becomes /nix.
		{Destination: "/nix", Type: "bind", Source: NixDir(), Options: []string{"rbind", "rw"}},
		// A bind of the host /proc rather than a fresh procfs. Mounting a new
		// procfs instance needs a pid namespace of our own, and inside an
		// unprivileged container whose /proc already has masked submounts the
		// kernel refuses it outright ("mount `proc` to `proc`: Operation not
		// permitted"). There is nothing to isolate here anyway - the process
		// is our own nix, not a user's job.
		{Destination: "/proc", Type: "bind", Source: "/proc", Options: []string{"rbind"}},
	}

	// Bind every top-level host directory so paths resolve identically inside
	// and out. Anything the caller named - a flake directory, a NAR file - is
	// referred to by its host path, so the mapping has to be the identity.
	entries, err := os.ReadDir("/")
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := "/" + e.Name()
		if name == "/nix" || name == "/proc" {
			continue
		}
		if e.Type()&os.ModeSymlink != 0 {
			// /bin -> usr/bin and friends: recreate the link rather than
			// bind through it, or the target is mounted at the wrong place.
			target, err := os.Readlink(name)
			if err != nil {
				continue
			}
			_ = os.Symlink(target, filepath.Join(rootfs, e.Name()))
			continue
		}
		if !e.IsDir() {
			continue
		}
		if err := os.MkdirAll(filepath.Join(rootfs, e.Name()), 0o755); err != nil {
			continue
		}
		opts := []string{"rbind", "ro"}
		if name == "/dev" || name == "/tmp" || name == "/run" {
			opts = []string{"rbind", "rw"}
		}
		mounts = append(mounts, mount{Destination: name, Type: "bind", Source: name, Options: opts})
	}
	// Home last so it overrides the read-only bind of whatever contains it.
	mounts = append(mounts, mount{
		Destination: home, Type: "bind", Source: home, Options: []string{"rbind", "rw"},
	})
	if err := os.MkdirAll(filepath.Join(rootfs, "nix"), 0o755); err != nil {
		return err
	}

	uid, gid := os.Getuid(), os.Getgid()
	spec := ociSpec{
		Version: "1.0.2",
		Process: process{
			// Mapped to uid 0 inside, which is what makes the store writable
			// without owning it as root outside.
			User: user{UID: 0, GID: 0},
			Args: args,
			Env: []string{
				"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
				"HOME=" + home,
				"USER=" + os.Getenv("USER"),
				"NIX_PAGER=cat",
				"NIX_CONF_DIR=" + ConfDir(),
				"SSL_CERT_FILE=" + sslCertFile(),
			},
			Cwd: cwd,
		},
		Root:   root{Path: "rootfs"},
		Mounts: mounts,
		Linux: linux{
			Namespaces:  []namespace{{Type: "user"}, {Type: "mount"}},
			UIDMappings: []idMapping{{ContainerID: 0, HostID: uid, Size: 1}},
			GIDMappings: []idMapping{{ContainerID: 0, HostID: gid, Size: 1}},
		},
	}

	data, err := json.MarshalIndent(spec, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "config.json"), data, 0o644)
}

// sslCertFile finds a CA bundle. Without one nix cannot talk to
// cache.nixos.org and every build turns into a source build, which on a
// worker looks like a hang rather than an error.
func sslCertFile() string {
	for _, p := range []string{
		"/etc/ssl/certs/ca-certificates.crt", // debian, ubuntu, arch
		"/etc/pki/tls/certs/ca-bundle.crt",   // fedora, rhel
		"/etc/ssl/ca-bundle.pem",             // suse
	} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

type ociSpec struct {
	Version string  `json:"ociVersion"`
	Process process `json:"process"`
	Root    root    `json:"root"`
	Mounts  []mount `json:"mounts"`
	Linux   linux   `json:"linux"`
}

type process struct {
	User user     `json:"user"`
	Args []string `json:"args"`
	Env  []string `json:"env"`
	Cwd  string   `json:"cwd"`
}

type user struct {
	UID int `json:"uid"`
	GID int `json:"gid"`
}

type root struct {
	Path string `json:"path"`
}

type mount struct {
	Destination string   `json:"destination"`
	Type        string   `json:"type"`
	Source      string   `json:"source"`
	Options     []string `json:"options,omitempty"`
}

type linux struct {
	Namespaces  []namespace `json:"namespaces"`
	UIDMappings []idMapping `json:"uidMappings"`
	GIDMappings []idMapping `json:"gidMappings"`
}

type namespace struct {
	Type string `json:"type"`
}

type idMapping struct {
	ContainerID int `json:"containerID"`
	HostID      int `json:"hostID"`
	Size        int `json:"size"`
}
