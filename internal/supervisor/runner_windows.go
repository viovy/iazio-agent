//go:build windows

package supervisor

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/viovy/iazio-agent/internal/procutil"
)

var (
	kernel32                     = syscall.NewLazyDLL("kernel32.dll")
	procCreateJobObjectW         = kernel32.NewProc("CreateJobObjectW")
	procSetInformationJobObject  = kernel32.NewProc("SetInformationJobObject")
	procAssignProcessToJobObject = kernel32.NewProc("AssignProcessToJobObject")
	procTerminateJobObject       = kernel32.NewProc("TerminateJobObject")
	procCloseHandle              = kernel32.NewProc("CloseHandle")
	procOpenProcess              = kernel32.NewProc("OpenProcess")
)

const (
	jobObjectExtendedLimitInformationClass = 9
	jobObjectLimitKillOnJobClose           = 0x2000
	processSetQuota                        = 0x0100
	processTerminate                       = 0x0001
)

type jobObjectBasicLimitInformation struct {
	PerProcessUserTimeLimit int64
	PerJobUserTimeLimit     int64
	LimitFlags              uint32
	MinimumWorkingSetSize   uintptr
	MaximumWorkingSetSize   uintptr
	ActiveProcessLimit      uint32
	Affinity                uintptr
	PriorityClass           uint32
	SchedulingClass         uint32
}

type ioCounters struct {
	ReadOperationCount  uint64
	WriteOperationCount uint64
	OtherOperationCount uint64
	ReadTransferCount   uint64
	WriteTransferCount  uint64
	OtherTransferCount  uint64
}

type jobObjectExtendedLimitInformation struct {
	BasicLimitInformation  jobObjectBasicLimitInformation
	IoInfo                 ioCounters
	ProcessMemoryLimit     uintptr
	JobMemoryLimit         uintptr
	PeakProcessMemoryLimit uintptr
	PeakJobMemoryLimit     uintptr
}

var (
	agentJobMu sync.Mutex
	agentJobs  = make(map[int]uintptr)
)

const createNoWindow = 0x08000000

func setProcGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP | createNoWindow
	cmd.SysProcAttr.HideWindow = true
}

func postStartProc(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil || cmd.Process.Pid <= 0 {
		return
	}
	r1, _, err := procCreateJobObjectW.Call(0, 0)
	if r1 == 0 {
		fmt.Fprintf(os.Stderr, "[agent] Warning: CreateJobObjectW failed for PID %d: %v\n", cmd.Process.Pid, err)
		return
	}
	jobHandle := r1
	info := jobObjectExtendedLimitInformation{
		BasicLimitInformation: jobObjectBasicLimitInformation{
			LimitFlags: 0, // Do NOT kill child process group when agent closes/restarts mid-run
		},
	}
	rSet, _, errSet := procSetInformationJobObject.Call(
		jobHandle,
		uintptr(jobObjectExtendedLimitInformationClass),
		uintptr(unsafe.Pointer(&info)),
		unsafe.Sizeof(info),
	)
	if rSet == 0 {
		fmt.Fprintf(os.Stderr, "[agent] Warning: SetInformationJobObject failed for PID %d: %v\n", cmd.Process.Pid, errSet)
	}
	rProc, _, errProc := procOpenProcess.Call(uintptr(processSetQuota|processTerminate), 0, uintptr(cmd.Process.Pid))
	if rProc != 0 {
		hProc := rProc
		rAssign, _, errAssign := procAssignProcessToJobObject.Call(jobHandle, hProc)
		if rAssign == 0 {
			fmt.Fprintf(os.Stderr, "[agent] Warning: AssignProcessToJobObject failed for PID %d: %v\n", cmd.Process.Pid, errAssign)
		}
		procCloseHandle.Call(hProc)
	} else {
		fmt.Fprintf(os.Stderr, "[agent] Warning: OpenProcess failed for Job Object binding on PID %d: %v\n", cmd.Process.Pid, errProc)
	}

	agentJobMu.Lock()
	agentJobs[cmd.Process.Pid] = jobHandle
	agentJobMu.Unlock()
}

func signalPID(pid int, sig string) error {
	if pid <= 0 {
		return nil
	}
	agentJobMu.Lock()
	jobHandle, ok := agentJobs[pid]
	if sig == "KILL" {
		delete(agentJobs, pid)
	}
	agentJobMu.Unlock()

	if ok && jobHandle != 0 {
		if sig == "KILL" {
			procTerminateJobObject.Call(jobHandle, 1)
			procCloseHandle.Call(jobHandle)
		}
	}
	return procutil.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(pid)).Run()
}

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	const processQueryLimitedInformation = 0x1000
	const stillActive = 259

	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(h)

	var exitCode uint32
	if err := syscall.GetExitCodeProcess(h, &exitCode); err != nil {
		return false
	}
	return exitCode == stillActive
}

func verifyProcessIdentity(pid int, expectedJobID string, startedAt time.Time) bool {
	if pid <= 0 || !isProcessAlive(pid) {
		return false
	}
	return !startedAt.IsZero() && time.Since(startedAt) < 24*time.Hour
}
