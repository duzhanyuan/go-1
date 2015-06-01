package nosql

import (
	"sync"

	"github.com/kingsoft-wps/go/nosql/ledis"
)

var newTestLedisOnce sync.Once

var testAddr = "redis:6379"
var testDB = 15
var testLedisClient *ledis.Client

func NewTestLedis() Store {
	f := func() {
		cfg := new(ledis.Config)
		cfg.Addr = testAddr
		cfg.MaxIdleConns = 4
		testLedisClient = ledis.NewClient(cfg)
	}
	newTestLedisOnce.Do(f)
	return &LedisStore{testLedisClient}
}

func NewTestRedis() (Store, error) {
	return NewRedisStore("redis", 6379, 8)
}
