# Disclaimer

**RTPARITYD IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.**

**IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH RTPARITYD OR THE USE OR OTHER DEALINGS IN RTPARITYD.**

---

## Data Loss Disclaimer

**You are solely responsible for:**
- Regular backups of your data and metadata
- Verifying data integrity using scrub and consistency check operations
- Testing restore procedures from backups
- Monitoring disk health and replacing failed disks promptly
- Ensuring adequate storage capacity

**rtparityd provides data protection through parity redundancy, journaled writes, and metadata snapshots, but:**

- **No guarantee** against data loss due to catastrophic failures, multiple simultaneous disk failures exceeding parity protection, or software bugs
- **Periodic scrubs** should be run to detect and repair silent data corruption
- **Backups** should be maintained for disaster recovery
- **Disk monitoring** should be in place to alert on drive failures

## Production Readiness

While rtparityd includes many production-grade features:
- Graceful shutdown with in-flight request draining
- Rate limiting and connection management
- Comprehensive health checks and metrics
- Audit logging and alert webhooks
- Automatic scrub scheduling

**It is your responsibility to:**
- Test in a non-production environment first
- Validate configuration for your specific use case
- Monitor system health and alerts
- Maintain off-site backups
- Plan for disaster recovery

---

**USE AT YOUR OWN RISK.**
