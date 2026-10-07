package workflowprobe

import (
	"net/http"

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
	var released domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+first.LeaseIDs[0]+"/release", nil, &released); err != nil {
		return err
	}
	if released.State != domain.LeaseReleased {
		return ensure(false, "lease release did not update state")
	}
	if err := harness.restart(); err != nil {
		return err
	}
	replayedAfterRestart, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if replayedAfterRestart.ID != first.ID {
		return ensure(false, "lease request replay after restart returned another group")
	}
	second, err := allocate(harness, plan.ID, "lease-request-0003", "east", 2)
	if err != nil {
		return err
	}
	if second.ID == first.ID {
		return ensure(false, "new request key reused the previous lease group")
	}
	if len(second.LeaseIDs) != 1 || second.LeaseIDs[0] == first.LeaseIDs[0] {
		return ensure(false, "new request key did not claim a fresh lease")
	}
	secondReplay, err := allocate(harness, plan.ID, "lease-request-0003", "east", 2)
	if err != nil {
		return err
	}
	if secondReplay.ID != second.ID {
		return ensure(false, "second lease request replay returned another group")
	}
	_, err = allocate(harness, plan.ID, "lease-request-0003", "east", 1)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}
	return nil
}
