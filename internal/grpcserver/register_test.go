package grpcserver

import (
	"testing"

	fleetforgev1 "github.com/launchverse/fleetforge/proto/fleetforge/v1"
)

func validRequest() *fleetforgev1.RegisterWorkerRequest {
	return &fleetforgev1.RegisterWorkerRequest{
		Hostname:   "build-node-1",
		InstanceId: "i-0123456789abcdef0",
		Os:         "linux/amd64",
		CpuCores:   16,
		MemoryMb:   65536,
		Version:    "1.0.0",
	}
}

func TestValidateRegisterRequest_Valid(t *testing.T) {
	if err := validateRegisterRequest(validRequest()); err != nil {
		t.Fatalf("expected valid request to pass, got: %v", err)
	}
}

func TestValidateRegisterRequest_MissingFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*fleetforgev1.RegisterWorkerRequest)
	}{
		{"missing hostname", func(r *fleetforgev1.RegisterWorkerRequest) { r.Hostname = "" }},
		{"missing instance_id", func(r *fleetforgev1.RegisterWorkerRequest) { r.InstanceId = "" }},
		{"missing os", func(r *fleetforgev1.RegisterWorkerRequest) { r.Os = "" }},
		{"missing version", func(r *fleetforgev1.RegisterWorkerRequest) { r.Version = "" }},
		{"zero cpu_cores", func(r *fleetforgev1.RegisterWorkerRequest) { r.CpuCores = 0 }},
		{"negative cpu_cores", func(r *fleetforgev1.RegisterWorkerRequest) { r.CpuCores = -1 }},
		{"zero memory_mb", func(r *fleetforgev1.RegisterWorkerRequest) { r.MemoryMb = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validRequest()
			tt.mutate(req)
			if err := validateRegisterRequest(req); err == nil {
				t.Fatalf("expected validation error for %s, got nil", tt.name)
			}
		})
	}
}

// This is the case doc 5.1's re-registration branch depends on being
// rejected at the transport boundary if it's ever wrong: a request with a
// negative capacity_slots shouldn't reach WorkerStore.Register and silently
// get clamped there without anyone noticing at the API layer. Validation
// intentionally does NOT check capacity_slots: 0/unset is a valid
// "default to 1" signal per the OpenAPI spec, so confirm that passes.
func TestValidateRegisterRequest_ZeroCapacitySlotsIsValid(t *testing.T) {
	req := validRequest()
	req.CapacitySlots = 0
	if err := validateRegisterRequest(req); err != nil {
		t.Fatalf("zero capacity_slots should be valid (defaults to 1 downstream), got: %v", err)
	}
}
