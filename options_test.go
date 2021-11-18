package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptionBaseOptionString(t *testing.T) {
	o := WithDatabase("database")

	assert.Equal(t, "WithDatabase", o.String())
}

func TestOptionWithCredentials(t *testing.T) {
	exp := Credentials{
		Database: "db",
		User:     "user",
		Password: "pass",
	}

	opt := WithCredentials(exp)

	opts := []ClientOption{opt}

	var res Credentials

	for _, o := range opts {
		if o, ok := o.(CredentialsOption); ok {
			err := o.F(&res)
			assert.NoError(t, err)
		}
	}

	assert.Equal(t, exp, res)
}

func TestOptionGetDatabase(t *testing.T) {
	opt := WithCredentials(Credentials{
		Database: "db0",
		User:     "user",
		Password: "pass",
	})
	db := GetDatabase(opt)

	assert.Equal(t, "db0", db)

	opt = WithDatabase("db1")
	db = GetDatabase(opt)

	assert.Equal(t, "db1", db)
}
