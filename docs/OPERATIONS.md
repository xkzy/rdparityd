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
snapshot-interval-hours = 24  # periodic metadata snapshots (0 to disable)

[server]
socket-path = "/var/run/rtparityd/rtparityd.sock"
require-root-user = true
read-timeout-seconds = 30
idle-timeout-seconds = 300
max-timeout-ms = 300000  # 5 minutes max per-request timeout

[limits]
write-rate-limit-per-second = 10
write-burst = 20
read-rate-limit-per-second = 50
read-burst = 100
max-goroutines = 64
slow-query-threshold-ms = 5000  # log warnings for slow queries

[logging]
level = "info"
json = true

[alert]
enabled = false
url = "http://localhost:9000/webhook"  # alert webhook URL

[scrub]
enabled = false
interval-hours = 24
start-hour = 2
end-hour = 5
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

### Memory Pool Stats
```bash
echo '{"id":"1","op":"memory-pool-stats"}' | socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

### Get Current Config
```bash
echo '{"id":"1","op":"config-get"}' | socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

### Pool Statistics
```bash
echo '{"id":"1","op":"pool-stats"}' | socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

### State Dump
```bash
echo '{"id":"1","op":"state-dump"}' | socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
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

## Idempotency

Write operations (`write`, `bulk-write`) support idempotency keys to prevent duplicate writes:

```bash
echo '{"id":"1","op":"write","idempotency_key":"unique-key-123","params":{"path":"/file","size":1024}}' | \
    socat - UNIX-CONNECT:/var/run/rtparityd/rtparityd.sock
```

If the same `idempotency_key` is used within 10 minutes, the cached result is returned without re-executing.

## Graceful Shutdown

When shutdown is requested:
1. Daemon stops accepting new connections
2. Waits for in-flight requests to complete
3. Exits cleanly

Use `drain` mode to stop accepting new writes while allowing reads to complete.

## Error Responses

Error responses include `error_code` and `suggestion` fields:

```json
{
  "id": "1",
  "error": "rate limit exceeded",
  "error_code": 1004,
  "suggestion": "retry after a short delay"
}
```

Error codes:
- 1001: Invalid parameters
- 1002: Not found
- 1003: Internal error
- 1004: Rate limited
- 1005: Timeout
- 1006: Not implemented
- 1007: Permission denied
- 1008: Invalid state
