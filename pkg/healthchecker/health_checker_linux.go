/*
Copyright 2020 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package healthchecker

import (
	"fmt"
	"github.com/coreos/go-systemd/sdjournal"
	"k8s.io/node-problem-detector/cmd/healthchecker/options"
	"k8s.io/node-problem-detector/pkg/healthchecker/types"
	"strings"
	"time"

	"github.com/coreos/go-systemd/dbus"
)

// getUptimeFunc returns the time for which the given service has been running.
func getUptimeFunc(service string) func() (time.Duration, error) {
	return func() (time.Duration, error) {
		conn, err := dbus.New()
		if err != nil {
			return time.Duration(0), fmt.Errorf("failed to connect to D-Bus: %w", err)
		}
		defer func() {
			if conn != nil {
				conn.Close()
			}
		}()

		unitName := service + ".service"
		props, err := conn.GetUnitProperties(unitName)
		if err != nil {
			return time.Duration(0), fmt.Errorf("failed to get properties for service %s: %w", unitName, err)
		}
		if props == nil {
			return time.Duration(0), fmt.Errorf("no properties found for service %s", unitName)
		}

		// Using InactiveExitTimestamp to capture the exact time when systemd tried starting the service. The service will
		// transition from inactive -> activating and the timestamp is captured.
		// Source : https://www.freedesktop.org/wiki/Software/systemd/dbus/
		// Using ActiveEnterTimestamp resulted in race condition where the service was repeatedly killed by plugin when
		// RestartSec of systemd and invoke interval of plugin got in sync. The service was repeatedly killed in
		// activating state and hence ActiveEnterTimestamp was never updated.
		inactiveExitTimestamp, ok := props["InactiveExitTimestamp"]
		if !ok {
			return time.Duration(0), fmt.Errorf("failed to retrieve the uptime for service %s", unitName)
		}

		val, ok := inactiveExitTimestamp.(string)
		if !ok {
			return time.Duration(0), fmt.Errorf("failed to parse the uptime for service %s", unitName)
		}

		t, err := time.Parse(time.RFC3339Nano, val)
		if err != nil {
			return time.Duration(0), fmt.Errorf("failed to parse the uptime timestamp for service %s: %w", unitName, err)
		}

		return time.Since(t), nil
	}
}

// getRepairFunc returns the repair function based on the component.
func getRepairFunc(hco *options.HealthCheckerOptions) func() {
	switch hco.Component {
	case types.DockerComponent:
		// Use "docker ps" for docker health check. Not using crictl for docker to remove
		// dependency on the kubelet.
		return func() {
			execCommand(types.CmdTimeout, "pkill", "-SIGUSR1", "dockerd")
			execCommand(types.CmdTimeout, "systemctl", "kill", "--kill-who=main", hco.Service)
		}
	default:
		// Just kill the service for all other components
		return func() {
			execCommand(types.CmdTimeout, "systemctl", "kill", "--kill-who=main", hco.Service)
		}
	}
}

// checkForPattern returns (true, nil) if logPattern occurs less than logCountThreshold number of times since last
// service restart. (false, nil) otherwise.
func checkForPattern(service, logStartTime, logPattern string, logCountThreshold int) (bool, error) {
	// Open the systemd journal for reading
	j, err := sdjournal.NewJournal()
	if err != nil {
		return false, err
	}
	defer func() {
		_ = j.Close()
	}()

	// Convert logStartTime to time.Time
	startTime, err := time.Parse(time.RFC3339, logStartTime)
	if err != nil {
		return false, err
	}

	// Set the starting position for reading journal entries
	if err := j.SeekRealtimeUsec(uint64(startTime.UnixNano() / 1000)); err != nil {
		return false, err
	}

	// Initialize the count of occurrences
	occurrences := 0

	// Read journal entries
	for {
		// Move to the next journal entry
		if _, err := j.Next(); err != nil {
			break
		}

		// Retrieve the log message field
		msg, err := j.GetData("_MESSAGE")
		if err != nil {
			return false, err
		}

		// Check if the log entry contains the pattern
		if strings.Contains(strings.ToLower(msg), strings.ToLower(logPattern)) {
			occurrences++
		}

		// Break the loop if the count threshold is reached
		if occurrences >= logCountThreshold {
			break
		}
	}

	if occurrences >= logCountThreshold {
		return false, nil
	}

	return true, nil
}

func getDockerPath() string {
	return "docker"
}
