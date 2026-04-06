#!/bin/bash
### BEGIN INIT INFO
# Provides:          rtparityd
# Required-Start:    $local_fs $remote_fs $network
# Required-Stop:     $local_fs $remote_fs $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: rtparityd storage engine
# Description:       Linux storage engine with real-time parity protection
### END INIT INFO

set -e

NAME=rtparityd
DESC="rtparityd storage engine"
DAEMON=/usr/local/bin/$NAME
SOCKET=/var/run/rtparityd/rtparityd.sock
METADATA=/var/lib/rtparityd/metadata.bin
JOURNAL=/var/lib/rtparityd/journal.bin
PIDFILE=/var/run/rtparityd/rtparityd.pid

[ -x "$DAEMON" ] || exit 0

. /lib/lsb/init-functions

case "$1" in
  start)
    log_daemon_msg "Starting $DESC" "$NAME"
    
    # Ensure directories exist
    mkdir -p /var/lib/rtparityd /var/run/rtparityd
    
    # Start daemon
    start-stop-daemon --start --pidfile "$PIDFILE" --make-pidfile \
        --background --exec "$DAEMON" -- \
        -socket "$SOCKET" \
        -metadata-path "$METADATA" \
        -journal-path "$JOURNAL" \
        -pool-name demo \
        -log /var/log/rtparityd.log
    
    log_end_msg 0
    ;;
    
  stop)
    log_daemon_msg "Stopping $DESC" "$NAME"
    start-stop-daemon --stop --pidfile "$PIDFILE" --remove-pidfile --retry 5
    log_end_msg 0
    ;;
    
  restart)
    $0 stop
    sleep 1
    $0 start
    ;;
    
  status)
    if [ -f "$PIDFILE" ]; then
        PID=$(cat "$PIDFILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "$NAME is running (pid $PID)"
            exit 0
        else
            echo "$NAME is not running (stale pidfile)"
            exit 1
        fi
    else
        echo "$NAME is not running"
        exit 3
    fi
    ;;
    
  *)
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
    ;;
esac

exit 0