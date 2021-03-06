package nosql

import (
	"testing"
)

func Test_RedisStore(t *testing.T) {
	redis, err := NewTestRedis()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err := redis.Set("key", "value"); err != nil {
		t.Fatal(err.Error())
	}

	s, err := redis.String(redis.Get("key"))

	if err != nil {
		t.Fatal(err.Error())
	}

	if s != "value" {
		t.Fatal("invalid value ", s)
	}

	if err := redis.HSet("abc", "efg", "hij"); err != nil {
		t.Fatal(err.Error())
	}
	size, err := redis.HLen("abc")

	if err != nil {
		t.Fatal(err.Error())
	}
	if size != 1 {
		t.Fatal("invalid size ", size)
	}
}

func TestSetStore(t *testing.T) {
	redis, err := NewTestRedis()
	if err != nil {
		t.Fatal(err.Error())
	}

	// clear key first.
	// in case of WRONGTYPE Operation against a key holding the wrong kind of value
	redis.Del("key")

	n, err := redis.SAdd("key", "m1", "m2")
	if err != nil {
		t.Fatal(err.Error())
	}
	if n != 2 {
		t.Fatal("n != 2")
	}

	ok, err := redis.SIsMember("key", "m1")
	if err != nil {
		t.Fatal(err.Error())
	}
	if !ok {
		t.Fatal("member not exists")
	}

	ok, err = redis.SIsMember("key", "m2")
	if err != nil {
		t.Fatal(err.Error())
	}
	if !ok {
		t.Fatal("member not exists")
	}

	n, err = redis.SRem("key", "m1", "m2")
	if err != nil {
		t.Fatal(err.Error())
	}
	if n != 2 {
		t.Fatal("number != 2")
	}

}
