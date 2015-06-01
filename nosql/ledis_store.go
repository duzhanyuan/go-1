package nosql

import (
	"errors"
	"fmt"

	"github.com/kingsoft-wps/go/nosql/ledis"
)

type LedisStore struct {
	ledis *ledis.Client
}

const (
	ledisMaxIdleConns = 32
)

func NewLedisStore(host string, port int, db int) (*LedisStore, error) {
	cfg := new(ledis.Config)
	cfg.Addr = fmt.Sprintf("%s:%d", host, port)
	cfg.MaxIdleConns = ledisMaxIdleConns
	store := &LedisStore{ledis.NewClient(cfg)}
	return store, store.Select(db)
}

func (l *LedisStore) Select(db int) error {
	_, err := l.ledis.Do("SELECT", db)
	return err
}

func (l *LedisStore) SetMaxIdle(maxIdle int) {
	//FIXME ledis client不能动态设置maxIdle
}

func (l *LedisStore) SetMaxActive(maxIdle int) {
	//FIXME ledis client不能动态设置maxActive
}

func (l *LedisStore) Get(key string) (interface{}, error) {
	return l.Bytes(l.ledis.Do("GET", key))
}

func (l *LedisStore) Set(key string, value interface{}) error {
	_, err := l.ledis.Do("SET", key, value)
	return err
}

func (l *LedisStore) SetNx(key string, value interface{}) (int64, error) {
	return l.Int64(l.ledis.Do("SETNX", key, value))
}

func (l *LedisStore) ExpireAt(key string, timestamp int64) (int64, error) {
	return l.Int64(l.ledis.Do("EXPIREAT", key, timestamp))
}

func (l *LedisStore) SetEx(key string, value interface{}, timeout int64) error {
	_, err := l.ledis.Do("SETEX", key, timeout, value)
	return err
}

func (l *LedisStore) Del(keys ...string) (int64, error) {
	ks := make([]interface{}, len(keys))
	for i, key := range keys {
		ks[i] = key
	}
	return l.Int64(l.ledis.Do("DEL", ks...))
}

func (l *LedisStore) Expire(key string, duration int64) (int64, error) {
	return ledis.Int64(l.ledis.Do("EXPIRE", key, duration))
}

func (l *LedisStore) Incr(key string) (int64, error) {
	return ledis.Int64(l.ledis.Do("INCR", key))
}

func (l *LedisStore) IncrBy(key string, delta int64) (int64, error) {
	return ledis.Int64(l.ledis.Do("INCRBY", key, delta))
}

func (l *LedisStore) HExpire(string, int64) error {
	//TODO
	panic("unimplemented")
	return nil
}

func (l *LedisStore) HGet(key string, field string) (interface{}, error) {
	return l.ledis.Do("HGET", key, field)
}

func (l *LedisStore) HSet(key string, field string, val interface{}) error {
	args := []interface{}{key, field, val}
	_, err := l.ledis.Do("HSET", args...)
	return err
}

func (l *LedisStore) HLen(key string) (int64, error) {
	return l.Int64(l.ledis.Do("HLEN", key))
}

func (l *LedisStore) HDel(key string, fields ...string) (int64, error) {
	ks := make([]interface{}, len(fields)+1)
	ks[0] = key
	for i, key := range fields {
		ks[i+1] = key
	}
	return l.Int64(l.ledis.Do("HDEL", ks...))
}

func (l *LedisStore) HClear(key string) error {
	_, err := l.ledis.Do("HCLEAR", key)
	return err
}

func (l *LedisStore) HMGet(key string, fields ...string) (interface{}, error) {
	if len(fields) == 0 {
		return nil, ErrNil
	}
	args := make([]interface{}, len(fields)+1)
	args[0] = key
	for i, field := range fields {
		args[i+1] = field
	}
	return l.ledis.Do("HMGET", args...)
}

func (l *LedisStore) HMSet(key string, kvs ...interface{}) error {
	if len(kvs) == 0 {
		return nil
	}
	if len(kvs)%2 != 0 {
		return errors.New("args num error")
	}
	args := make([]interface{}, len(kvs)+1)
	args[0] = key
	for i := 0; i < len(kvs); i += 2 {
		if _, ok := kvs[i].(string); !ok {
			return errors.New("field must be string")
		}
		args[i+1] = kvs[i]
		args[i+2] = kvs[i+1]
	}
	_, err := l.ledis.Do("HMSET", args...)
	return err
}

func (l *LedisStore) HGetAll(string) (map[string]interface{}, error) {
	//TODO
	panic("unimplemented")
	return nil, nil
}

func (l *LedisStore) HIncrBy(key, field string, delta int64) (int64, error) {
	return l.Int64(l.ledis.Do("HINCRBY", key, field, delta))
}

func (l *LedisStore) ZAdd(key string, kvs ...interface{}) (int64, error) {
	if len(kvs) == 0 {
		return 0, nil
	}
	if len(kvs)%2 != 0 {
		return 0, errors.New("args num error")
	}
	args := make([]interface{}, len(kvs)+1)
	args[0] = key
	for i := 0; i < len(kvs); i += 2 {
		if _, ok := kvs[i].(string); !ok {
			return 0, errors.New("field must be string")
		}
		args[i+1] = kvs[i]
		args[i+2] = kvs[i+1]
	}
	_, err := l.ledis.Do("HMSET", args...)
	return 0, err
}

func (l *LedisStore) ZClear(key string) error {
	_, err := l.ledis.Do("ZCLEAR", key)
	return err
}

func (l *LedisStore) ZRem(string, ...string) (int64, error) {
	//TODO
	panic("unimplemented")
	return 0, nil
}

func (l *LedisStore) ZExpire(string, int64) error {
	//TODO
	panic("unimplemented")
	return nil
}
func (l *LedisStore) ZRange(string, int64, int64, bool) (interface{}, error) {
	//TODO
	panic("unimplemented")
	return nil, nil
}

func (l *LedisStore) ZRangeByScoreWithScore(key string, min, max int64) (map[string]int64, error) {
	panic("unimplemented")
	return nil, nil
}

func (l *LedisStore) LRange(key string, start, stop int64) (interface{}, error) {
	panic("unimplemented")
	return nil, nil
}

func (l *LedisStore) LLen(key string) (int64, error) {
	panic("unimplemented")
	return 0, nil
}

func (l *LedisStore) LPop(key string) (interface{}, error) {
	panic("unimplemented")
	return nil, nil
}

func (l *LedisStore) RPush(key string, value ...interface{}) error {
	panic("unimplemented")
	return nil
}

func (l *LedisStore) SAdd(key string, members ...interface{}) (int64, error) {
	panic("unimplemented")
	return 0, nil
}

func (l *LedisStore) SIsMember(key string, member interface{}) (bool, error) {
	panic("unimplemented")
	return false, nil
}

func (l *LedisStore) SRem(key string, members ...interface{}) (int64, error) {
	panic("unimplemented")
	return 0, nil
}
