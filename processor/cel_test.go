//go:build ignore

package processor

import (
	"testing"

	"github.com/google/cel-go/common"
	"github.com/stretchr/testify/require"
)

func TestCel(t *testing.T) {
	prg, err := compileCel(common.NewTextSource(`
.query
`))
	require.NoError(t, err)

	ret, det, err := prg.Eval(map[string]interface{}{
		"query": map[string]interface{}{
			"query":      "SELECT 1",
			"compressed": true,
		},
	})
	require.NoError(t, err)

	t.Logf("ret: %v (%[1]T) (%v)  details: %v", ret, ret.Value(), det)
}
