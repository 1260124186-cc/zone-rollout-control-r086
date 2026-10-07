package workflowprobe

import (
	"net/http"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runNodeLeaseAllocation() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-east-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-east-02", "east"); err != nil {
		return err
	}
	plan, _, err := createActivePlan(harness, "lease.example", 1001, 2, 0.5, 100)
	if err != nil {
		return err
	}
	first, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if len(first.LeaseIDs) != 2 {
		return ensure(false, "first lease group must contain two leases")
	}
	replay, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if replay.ID != first.ID {
		return ensure(false, "lease request replay returned another group")
	}
	_, err = allocate(harness, plan.ID, "lease-request-0002", "east", 2)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}
	if err := release(harness, first.LeaseIDs[0]); err != nil {
		return err
	}

	// A fresh request key is an independent operation: with one node released,
	// identical region/duration/limit must claim that node instead of replaying
	// the original group.
	second, err := allocate(harness, plan.ID, "lease-request-0003", "east", 2)
	if err != nil {
		return err
	}
	if second.ID == first.ID {
		return ensure(false, "new request key returned the previous lease group")
	}
	if len(second.LeaseIDs) != 1 {
		return ensure(false, "new request key did not claim the released node")
	}
	if second.LeaseIDs[0] == first.LeaseIDs[0] || second.LeaseIDs[0] == first.LeaseIDs[1] {
		return ensure(false, "new request key reused a lease from the previous group")
	}
	_, err = allocate(harness, plan.ID, "lease-request-0004", "east", 2)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}

	// Reusing a request key with different business parameters must conflict.
	_, err = allocateLease(harness, plan.ID, "lease-request-0001", "east", 2, 900)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}
	_, err = allocateLease(harness, plan.ID, "lease-request-0001", "west", 2, 600)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}

	// Expiry must return nodes to the pool: free both nodes, take short leases
	// under a new key, let them expire, and confirm the next new key claims the
	// reclaimed nodes in a fresh group instead of replaying the expired one.
	if err := release(harness, first.LeaseIDs[1]); err != nil {
		return err
	}
	if err := release(harness, second.LeaseIDs[0]); err != nil {
		return err
	}
	short, err := allocateLease(harness, plan.ID, "lease-request-0005", "east", 2, 60)
	if err != nil {
		return err
	}
	if len(short.LeaseIDs) != 2 {
		return ensure(false, "short lease group did not claim both free nodes")
	}
	harness.clock.Advance(61 * time.Second)
	afterExpiry, err := allocateLease(harness, plan.ID, "lease-request-0006", "east", 2, 60)
	if err != nil {
		return err
	}
	if afterExpiry.ID == first.ID || afterExpiry.ID == short.ID {
		return ensure(false, "post-expiry request returned a previous lease group")
	}
	if len(afterExpiry.LeaseIDs) != 2 {
		return ensure(false, "expired nodes were not reclaimed by the new request")
	}

	// Idempotency semantics must survive a service restart backed by the same
	// snapshot: the original key still replays, conflicting payloads still fail,
	// and a new key stays an independent operation.
	if err := harness.restart(); err != nil {
		return err
	}
	replayed, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if replayed.ID != first.ID {
		return ensure(false, "request key replay changed after restart")
	}
	_, err = allocateLease(harness, plan.ID, "lease-request-0001", "east", 2, 900)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}
	_, err = allocate(harness, plan.ID, "lease-request-0007", "east", 2)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}
	return nil
}

func release(harness *harness, leaseID string) error {
	var released domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+leaseID+"/release", nil, &released); err != nil {
		return err
	}
	if released.State != domain.LeaseReleased {
		return ensure(false, "lease release did not update state")
	}
	return nil
}
