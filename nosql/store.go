package nosql

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

var ErrNil = errors.New("nil return")
var ErrWrongType = errors.New("wrong type")
var ErrWrongArgsNum = errors.New("args num error")

type Reply interface {
	Int(interface{}, error) (int, error)
	Int64(interface{}, error) (int64, error)
	Uint64(interface{}, error) (uint64, error)
	Float64(interface{}, error) (float64, error)
	Bool(interface{}, error) (bool, error)
	Bytes(interface{}, error) ([]byte, error)
	String(interface{}, error) (string, error)
	Strings(interface{}, error) ([]string, error)
	Values(interface{}, error) ([]interface{}, error)
}

type kvStore interface {
	Get(string) (interface{}, error)
	Set(string, interface{}) error
	SetEx(string, interface{}, int64) error
	SetNx(string, interface{}) (int64, error)
	Del(...string) (int64, error)
	Incr(string) (int64, error)
	IncrBy(string, int64) (int64, error)
	Expire(string, int64) (int64, error)
	ExpireAt(string, int64) (int64, error)
}

type KVStore interface {
	Reply
	kvStore
}

type hashStore interface {
	HGet(string, string) (interface{}, error)
	HSet(string, string, interface{}) error
	HMGet(string, ...string) (interface{}, error)
	HMSet(string, ...interface{}) error
	HExpire(string, int64) error
	HGetAll(string) (map[string]interface{}, error)
	HIncrBy(string, string, int64) (int64, error)
	HDel(string, ...string) (int64, error)
	HClear(string) error
	HLen(string) (int64, error)
}

type HashStore interface {
	Reply
	hashStore
}

type setStore interface {
	SAdd(string, ...interface{}) (int64, error)
	SIsMember(string, interface{}) (bool, error)
	SRem(string, ...interface{}) (int64, error)
}

type SetStore interface {
	Reply
	setStore
}

type zSetStore interface {
	ZAdd(string, ...interface{}) (int64, error)
	ZRem(string, ...string) (int64, error)
	ZExpire(string, int64) error
	ZRange(string, int64, int64, bool) (interface{}, error)
	ZRangeByScoreWithScore(string, int64, int64) (map[string]int64, error)
	ZClear(string) error
}

type ZSetStore interface {
	Reply
	zSetStore
}

type listStore interface {
	LRange(string, int64, int64) (interface{}, error)
	LLen(string) (int64, error)
	LPop(string) (interface{}, error)
	RPush(string, ...interface{}) error
}

type ListStore interface {
	Reply
	listStore
}

type HashZSetStore interface {
	Reply
	hashStore
	zSetStore
}

type KVHashStore interface {
	Reply
	kvStore
	hashStore
}

type Store interface {
	Reply
	kvStore
	hashStore
	zSetStore
	listStore
	setStore
	SetMaxIdle(int)
	SetMaxActive(int)
}

func Open(connurl string) (Store, error) {
	scheme, host, port, db, err := parseConnurl(connurl)
	if err != nil {
		return nil, err
	}
	switch scheme {
	case "redis":
		return NewRedisStore(host, port, db)
	case "ledis":
		return NewLedisStore(host, port, db)
	case "memory":
		return NewMemoryStore()
	default:
		return nil, fmt.Errorf("invalid connection url %s", connurl)

	}

}

func parseConnurl(connurl string) (scheme string, host string, port int, db int, err error) {
	url, err := url.Parse(connurl)
	if err != nil {
		return
	}
	scheme = url.Scheme
	if scheme != "ledis" && scheme != "redis" && scheme != "memory" {
		err = fmt.Errorf("invalid connection url %s", connurl)
		return
	}
	if scheme == "memory" {
		return
	}
	parts := strings.SplitN(url.Host, ":", 2)
	if len(parts) != 2 {
		err = fmt.Errorf("invalid connection url %s", connurl)
		return
	}
	host = parts[0]
	if port, err = strconv.Atoi(parts[1]); err != nil {
		return
	}
	path := strings.Trim(url.Path, "/")
	if db, err = strconv.Atoi(path); err != nil {
		return
	}
	return
}
