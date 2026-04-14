package remote

import (
	"fmt"
	"strings"
)

type SSHConfig struct {
	User string
	Host string
	Port int
}

func Parse(remote string) (*SSHConfig, error) {
	var user, host string
	var port int

	if strings.Contains(remote, "@") {
		parts := strings.SplitN(remote, "@", 2)
		user = parts[0]
		hostPort := parts[1]
		if strings.Contains(hostPort, ":") {
			hP := strings.SplitN(hostPort, ":", 2)
			host = hP[0]
			fmt.Sscanf(hP[1], "%d", &port)
		} else {
			host = hostPort
			port = 22
		}
	} else {
		user = "root"
		if strings.Contains(remote, ":") {
			hP := strings.SplitN(remote, ":", 2)
			host = hP[0]
			fmt.Sscanf(hP[1], "%d", &port)
		} else {
			host = remote
			port = 22
		}
	}

	return &SSHConfig{User: user, Host: host, Port: port}, nil
}
