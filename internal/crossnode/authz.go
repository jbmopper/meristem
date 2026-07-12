package crossnode

import (
	"errors"
	"fmt"

	"github.com/jbmopper/meristem/internal/domain"
)

const OperationClassWorkItemsWrite = "work_items.write"

var (
	ErrCommandRootForbidden = errors.New("crossnode: root token cannot authorize peer delivery")
	ErrCommandScopeDenied   = errors.New("crossnode: token lacks required peer-delivery scope")
)

// QueueWriteScope returns the exact queue-host scope that authorizes one
// operation class for one target. A token for another target or operation
// class is deliberately not interchangeable.
func QueueWriteScope(targetNodeID, operationClass string) string {
	return fmt.Sprintf("crossnode.queue:%s:%s", targetNodeID, operationClass)
}

// QueueDrainScope returns the scope a target uses to poll only its own queue.
func QueueDrainScope(targetNodeID string) string {
	return "crossnode.drain:" + targetNodeID
}

// QueueAckScope returns the scope a target uses to acknowledge only its own
// queued commands.
func QueueAckScope(targetNodeID string) string {
	return "crossnode.ack:" + targetNodeID
}

// QueueOutcomeReadScope authorizes an origin to read only terminal queue
// outcomes whose immutable origin_node_id matches that origin.
func QueueOutcomeReadScope(originNodeID string) string {
	return "crossnode.outcomes:" + originNodeID
}

// OutcomeObserveScope authorizes one origin-local actor to observe outcomes
// from one pinned queue host. It grants no remote mutation authority.
func OutcomeObserveScope(queueHostNodeID, originNodeID string) string {
	return fmt.Sprintf("crossnode.observe:%s:%s", queueHostNodeID, originNodeID)
}

// OriginScope binds a peer-delivery credential to the origin node it may
// assert. Syntax validation alone is not an authentication boundary.
func OriginScope(originNodeID string) string { return "crossnode.origin:" + originNodeID }

// TargetExecuteScope authorizes a dedicated target-local token to execute
// authenticated remote envelopes without granting that authority to ordinary
// local work-item writers.
func TargetExecuteScope(targetNodeID string) string { return "crossnode.execute:" + targetNodeID }

// OperationClassForCommandPath validates a Stage 1 queued command path and
// maps it to the narrow scope class used at the queue-host boundary.
func OperationClassForCommandPath(commandPath string) (string, error) {
	if err := ValidateCommandPath(commandPath); err != nil {
		return "", err
	}
	return OperationClassWorkItemsWrite, nil
}

// AuthorizeQueueWrite fails closed for root, revoked, legacy unscoped, and
// wrong-target tokens. Cross-node credentials are explicit capabilities and
// do not inherit the local legacy-unscoped compatibility shortcut.
func AuthorizeQueueWrite(actor domain.Token, targetNodeID, originNodeID, commandPath string) error {
	operationClass, err := OperationClassForCommandPath(commandPath)
	if err != nil {
		return err
	}
	if err := authorizeExactScope(actor, QueueWriteScope(targetNodeID, operationClass)); err != nil {
		return err
	}
	return authorizeExactScope(actor, OriginScope(originNodeID))
}

func AuthorizeQueueDrain(actor domain.Token, targetNodeID string) error {
	return authorizeExactScope(actor, QueueDrainScope(targetNodeID))
}

func AuthorizeQueueAck(actor domain.Token, targetNodeID string) error {
	return authorizeExactScope(actor, QueueAckScope(targetNodeID))
}

func AuthorizeQueueOutcomeRead(actor domain.Token, originNodeID string) error {
	return authorizeExactScope(actor, QueueOutcomeReadScope(originNodeID))
}

func AuthorizeOutcomeObserve(actor domain.Token, queueHostNodeID, originNodeID string) error {
	return authorizeExactScope(actor, OutcomeObserveScope(queueHostNodeID, originNodeID))
}

func AuthorizeTargetExecution(actor domain.Token, targetNodeID, originNodeID string) error {
	if err := authorizeExactScope(actor, TargetExecuteScope(targetNodeID)); err != nil {
		return err
	}
	return authorizeExactScope(actor, OriginScope(originNodeID))
}

func authorizeExactScope(actor domain.Token, required string) error {
	if actor.IsRoot {
		return ErrCommandRootForbidden
	}
	if actor.RevokedAt != nil || required == "" {
		return ErrCommandScopeDenied
	}
	for _, scope := range actor.Scopes {
		if scope == required {
			return nil
		}
	}
	return ErrCommandScopeDenied
}
