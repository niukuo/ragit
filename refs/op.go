package refs

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-git/go-git/v5/plumbing"
)

func ValidateTarget(hash []byte) bool {
	return len(hash) == 20 || hash == nil
}

func Validate(oplog Oplog) error {
	deleteOnly := true
	for _, op := range oplog.GetOps() {
		name := op.GetName()
		if !strings.HasPrefix(name, "refs/") {
			return fmt.Errorf("invalid refs: %s", name)
		}
		if target := op.GetOldTarget(); !ValidateTarget(target) {
			return fmt.Errorf("invalid old target, name: %s, target: %s",
				name, string(target))
		}
		if target := op.GetTarget(); !ValidateTarget(target) {
			return fmt.Errorf("invalid target, name: %s, target: %s",
				name, string(target))
		}
		var target plumbing.Hash
		copy(target[:], op.GetTarget())
		if !target.IsZero() {
			deleteOnly = false
		}
	}

	hasPack := len(oplog.ObjPack) > 0 || len(oplog.Objs) > 0 || len(oplog.Params) > 0

	if deleteOnly && hasPack {
		return errors.New("The packfile MUST NOT be sent if the only command used is 'delete'")
	}

	return nil
}
