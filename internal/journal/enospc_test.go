package journal

import (
	"testing"
)

func TestENOSPCCleanup(t *testing.T) {
	t.Skip("ENOSPC injection requires FaultInjector interface")
}

func TestWriteWithDiskFull(t *testing.T) {
	t.Skip("ENOSPC injection requires FaultInjector interface")
}

func TestRecoveryAfterENOSPC(t *testing.T) {
	t.Skip("ENOSPC injection requires FaultInjector interface")
}
