package nosql

import (
	"testing"
)

func Test_LeidStore(t *testing.T) {
	ledis := NewTestLedis()
	err := ledis.Set("key", "value")
	if err != nil {
		t.Fatal(err.Error())
	}

	s, err := ledis.String(ledis.Get("key"))

	if err != nil {
		t.Fatal(err.Error())
	}

	if s != "value" {
		t.Fatal("invalid value ", s)
	}
	if err := ledis.HSet("abc", "efg", "hij"); err != nil {
		t.Fatal(err.Error())
	}
	size, err := ledis.HLen("abc")

	if err != nil {
		t.Fatal(err.Error())
	}
	if size != 1 {
		t.Fatal("invalid size ", size)
	}

}
