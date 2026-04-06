# rtparityd Operations Guide

## Quick Start

```bash
# Start the daemon
systemctl start rtparityd

# Check status
systemctl status rtparityd
rtparityd --socket /var/run/rtparityd/rtparityd.sock status

# View logs
journalctl -u rtparityd -f
```

## Configuration

Edit `/etc/rtparityd/rtparityd.conf`:

```toml
[global]
pool-name = "demo"
journal-path = "/var/lib/rtparityd/journal.bin"
metadata-path = "/var/lib/rtparityd/metadata.bin"

[server]
socket-path = "/var/run/rtparityd/rtparityd.sock"
require-root-user = true
read-timeout-seconds = 30

[limits]
write-rate-limit-per-second = 10
read-rate-limit-per-second = 50
max-goroutines = 64

[logging]
level = "info"
json = true

[scrub]
enabled = false
interval-hours = 24
```

Reload config without restart:
```bash
echo '{"id":"1","op":"reload-config","params":{"config_path":"/etc/rtparityd/rtparityd.conf"}}' | \
    socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

## Common Operations

### Health Check
```bash
echo '{"id":"1","op":"health"}' | socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

### Get Metrics (JSON)
```bash
echo '{"id":"1","op":"metrics"}' | socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

### Get Metrics (Prometheus)
```bash
echo '{"id":"1","op":"prometheus"}' | socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

### Disk Statistics
```bash
echo '{"id":"1","op":"disk-stats"}' | socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

### Backup
```bash
echo '{"id":"1","op":"backup","params":{"output_path":"/backup/2024-01-01"}}' | \
    socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

### Restore
```bash
echo '{"id":"1","op":"restore","params":{"input_path":"/backup/2024-01-01"}}' | \
    socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

### Scrub
```bash
echo '{"id":"1","op":"scrub","params":{"repair":false}}' | \
    socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

### Rebuild (single disk)
```bash
echo '{"id":"1","op":"rebuild","params":{"disk_id":"disk-03"}}' | \
    socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

## Admin Operations

Admin socket requires root access:

```bash
# Shutdown daemon
echo '{"id":"1","op":"shutdown"}' | socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd-admin.sock

# Drain mode (stops accepting writes)
echo '{"id":"1","op":"drain"}' | socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd-admin.sock

# Force garbage collection
echo '{"id":"1","op":"force-gc"}' | socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd-admin.sock
```

## Troubleshooting

### Check for startup errors
```bash
journalctl -u rtparityd -n 50 | grep -i error
```

### Check lock files
```bash
ls -la /var/lib/rtparityd/*.lock
```

### Run integrity check
```bash
rtpctl check-invariants --metadata /var/lib/rtparityd/metadata.bin
```

### View audit logs
```bash
tail -f /var/log/rtparityd/audit.log
```

### Manual recovery
```bash
# Stop daemon
systemctl stop rtparityd

# Check journal
rtpctl journal-demo --journal /var/lib/rtparityd/journal.bin

# Restart
systemctl start rtparityd
```

## Performance Tuning

### Increase rate limits
```toml
[limits]
write-rate-limit-per-second = 50
write-burst = 100
read-rate-limit-per-second = 100
read-burst = 200
```

### Enable automatic scrub
```toml
[scrub]
enabled = true
interval-hours = 24
start-hour = 2
end-hour = 5
```

## Security

- Control socket: 0660, requires root or group member
- Admin socket: 0660, root only (SO_PEERCRED)
- Audit logs: 0640, owned by root
- Never run with `--require-root=false` in production
