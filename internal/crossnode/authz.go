package crossnode

import (
	"errors"
	"fmt"

	"github.com/jbmopper/meristem/internal/domain"
)

const OperationClassWorkItemsWrite = "work_items.write"

const (
	// HeaderTargetNode binds a peer REST request to its expected terminating
	// node. It is structural routing metadata, not actor identity.
	HeaderTargetNode = "X-Meristem-Target-Node"
	// HeaderOriginNode names the peer that originated a request. Receivers
	// validate and record it as provenance but never treat it as local actor
	// identity.
	HeaderOriginNode = "X-Meristem-Origin-Node"
)

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
func AuthorizeQueueWrite(actor domain.Token, targetNodeID, commandPath string) error {
	operationClass, err := OperationClassForCommandPath(commandPath)
	if err != nil {
		return err
	}
	return authorizeExactScope(actor, QueueWriteScope(targetNodeID, operationClass))
}

func AuthorizeQueueDrain(actor domain.Token, targetNodeID string) error {
	return authorizeExactScope(actor, QueueDrainScope(targetNodeID))
}

func AuthorizeQueueAck(actor domain.Token, targetNodeID string) error {
	return authorizeExactScope(actor, QueueAckScope(targetNodeID))
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
