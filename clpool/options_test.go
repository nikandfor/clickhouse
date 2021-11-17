package clpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptionString(t *testing.T) {
	o := WithAgentName("agent_name")

	assert.Equal(t, "WithAgentName", o.String())
}
