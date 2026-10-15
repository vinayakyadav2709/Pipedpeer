#!/data/data/com.termux/files/usr/bin/bash
# Turn off Android's phantom process killer from Termux itself, no laptop:
# Android 12+ kills app child processes past 32, which kills pool workers.
# It uses Wireless debugging on the phone's own adb port (Android 11+).
#
# Before running: Settings > Developer options > Wireless debugging: on, then
# "Pair device with pairing code". Put Termux and Settings in split screen
# (the pairing dialog closes when Settings leaves the screen).
#
#   bash phone-phantom-off.sh [PAIR_PORT PAIR_CODE CONNECT_PORT]
#
# PAIR_PORT and PAIR_CODE are on the pairing dialog; CONNECT_PORT is the port
# under "IP address & Port" on the Wireless debugging screen. Asked when not
# given. On Android 14+ the Developer options toggle "Disable child process
# restrictions" does the same with no adb. From a laptop instead:
#   adb shell device_config put activity_manager max_phantom_processes 2147483647
#   adb shell settings put global settings_enable_monitor_phantom_procs false
set -euo pipefail

command -v adb >/dev/null || pkg install -y android-tools

pair_port=${1:-}; code=${2:-}; port=${3:-}
[ -n "$pair_port" ] || read -rp "pairing port (Pair device with pairing code dialog): " pair_port
[ -n "$code" ] || read -rp "pairing code: " code
adb pair "127.0.0.1:$pair_port" "$code"
[ -n "$port" ] || read -rp "port under 'IP address & Port' on the Wireless debugging screen: " port
adb connect "127.0.0.1:$port"
dev="127.0.0.1:$port"
adb -s "$dev" shell device_config set_sync_disabled_for_tests persistent || echo "phone-phantom-off: set_sync_disabled_for_tests failed (Android < 12?); the setting may reset on the next config sync" >&2
adb -s "$dev" shell device_config put activity_manager max_phantom_processes 2147483647
adb -s "$dev" shell settings put global settings_enable_monitor_phantom_procs false
echo "max_phantom_processes = $(adb -s "$dev" shell device_config get activity_manager max_phantom_processes)"
adb disconnect "$dev" >/dev/null
echo "phantom process killer off; Wireless debugging can be turned off again"
