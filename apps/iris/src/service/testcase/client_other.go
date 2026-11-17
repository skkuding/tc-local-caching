//go:build !linux

package testcase

import (
	"context"
	"fmt"
)

func GetVersionedTestcase(_ context.Context, _, _, _ string, _ TestcaseFilterCode) (Testcase, error) {
	return Testcase{}, fmt.Errorf("TC Manager descriptor transport requires Linux")
}
