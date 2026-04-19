package execution

import (
	"os"
	"path/filepath"
	"strings"
)

func ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

func BuildRunCommand(runPath, jobDir string, isolate bool, scriptRelPath string, scriptArgs []string, envs []string) string {
	workDir := filepath.Join(jobDir, "work")

	var runCmdBuilder strings.Builder
	for _, env := range envs {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) == 2 {
			runCmdBuilder.WriteString("export " + parts[0] + "=" + ShellQuote(parts[1]) + " && ")
		} else {
			runCmdBuilder.WriteString("export " + parts[0] + "=" + ShellQuote(os.Getenv(parts[0])) + " && ")
		}
	}
	runCmdBuilder.WriteString(ShellQuote(runPath) + " " + ShellQuote(scriptRelPath))
	for _, arg := range scriptArgs {
		runCmdBuilder.WriteString(" " + ShellQuote(arg))
	}
	runCmdTarget := runCmdBuilder.String()

	if !isolate {
		return "mkdir -p " + ShellQuote(workDir) + " && cd " + ShellQuote(workDir) + " && " + runCmdTarget
	}

	homeDir := filepath.Join(jobDir, "home")
	pathEnv := "/nix/var/nix/profiles/default/bin:/nix/var/nix/profiles/default/sbin:/root/.nix-profile/bin"

	bwrapArgs := []string{
		"bwrap",
		"--die-with-parent",
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
	}

	for _, env := range envs {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) == 2 {
			bwrapArgs = append(bwrapArgs, "--setenv "+parts[0]+" "+ShellQuote(parts[1]))
		} else {
			bwrapArgs = append(bwrapArgs, "--setenv "+parts[0]+" "+ShellQuote(os.Getenv(parts[0])))
		}
	}

	bwrapArgs = append(bwrapArgs, "--", ShellQuote(runPath), ShellQuote(scriptRelPath))
	for _, arg := range scriptArgs {
		bwrapArgs = append(bwrapArgs, ShellQuote(arg))
	}

	return "mkdir -p " + ShellQuote(workDir) + " " + ShellQuote(homeDir) + " && " + strings.Join(bwrapArgs, " ")
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
