package utils

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

var entropyPool = sync.Pool{
	New: func() interface{} {
		// Monotonic entropy защищает от создания одинаковых ULID в одну миллисекунду
		return ulid.Monotonic(rand.Reader, 0)
	},
}

func GenerateULID() string {
	e := entropyPool.Get().(*ulid.MonotonicEntropy)
	defer entropyPool.Put(e)

	return ulid.MustNew(ulid.Timestamp(time.Now()), e).String()
}
