package execution

import (
	"path/filepath"
	"strings"
)

func ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

func BuildRunCommand(runPath, jobDir string, isolate bool) string {
	workDir := filepath.Join(jobDir, "work")

	if !isolate {
		return "mkdir -p " + ShellQuote(workDir) + " && cd " + ShellQuote(workDir) + " && " + ShellQuote(runPath)
	}

	homeDir := filepath.Join(jobDir, "home")
	pathEnv := "/nix/var/nix/profiles/default/bin:/nix/var/nix/profiles/default/sbin:/root/.nix-profile/bin"

	return strings.Join([]string{
		"mkdir -p " + ShellQuote(workDir) + " " + ShellQuote(homeDir),
		"bwrap",
		"--die-with-parent",
		"--unshare-net",
		"--unshare-pid",
		"--unshare-ipc",
		"--unshare-uts",
		"--ro-bind /nix /nix",
		"--dev /dev",
		"--proc /proc",
		"--tmpfs /tmp",
		"--bind " + ShellQuote(workDir) + " /work",
		"--bind " + ShellQuote(homeDir) + " /home/root",
		"--chdir /work",
		"--setenv HOME /home/root",
		"--setenv PATH " + ShellQuote(pathEnv),
		"-- " + ShellQuote(runPath),
	}, " ")
}

func BuildDetachedRemoteCommand(runCmd, jobDir, storePath, resolvedJobName string) string {
	stdoutPath := filepath.Join(jobDir, "stdout.log")
	stderrPath := filepath.Join(jobDir, "stderr.log")
	pidPath := filepath.Join(jobDir, "pid")
	storePathFile := filepath.Join(jobDir, "store_path")
	donePath := filepath.Join(jobDir, "done")
	exitCodePath := filepath.Join(jobDir, "exit_code")

	runner := strings.Join([]string{
		runCmd,
		"status=$?",
		"echo $status > " + ShellQuote(exitCodePath),
		"touch " + ShellQuote(donePath),
		"exit $status",
	}, " ; ")

	return strings.Join([]string{
		"mkdir -p " + ShellQuote(jobDir),
		"nohup sh -lc " + ShellQuote(runner) + " >" + ShellQuote(stdoutPath) + " 2>" + ShellQuote(stderrPath) + " < /dev/null &",
		"echo $! > " + ShellQuote(pidPath),
		"echo " + ShellQuote(storePath) + " > " + ShellQuote(storePathFile),
		"echo JOB_NAME=" + ShellQuote(resolvedJobName),
		"echo JOB_DIR=" + ShellQuote(jobDir),
		"echo PID=$(cat " + ShellQuote(pidPath) + ")",
		"echo STORE_PATH=" + ShellQuote(storePath),
		"echo DONE_PATH=" + ShellQuote(donePath),
		"echo EXIT_CODE_PATH=" + ShellQuote(exitCodePath),
		"echo STDOUT=" + ShellQuote(stdoutPath),
		"echo STDERR=" + ShellQuote(stderrPath),
	}, " && ")
}
