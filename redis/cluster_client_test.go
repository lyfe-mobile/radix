package redis
import (
		"github.com/fzzy/radix/redis"
		"testing"
		"time"
		
	)

func TestCmd(t *testing.T) {
	nodes := []string{"localhost:7000", "localhost:7001", "localhost:7002" , "localhost:7003", "localhost:7004", "localhost:7005"}
	c, _ := redis.DialCluster(nodes)
	v := c.Cmd("set", "hello", "world")
	time.Sleep(10 * time.Second)
	v = c.Cmd("get", "hello")
	retVal, err := v.Str()
	if err != nil {
		t.Error("Failure", err, v)
		return
	}

	if retVal != "world" {
		t.Error("Return value should be 'world' not", retVal)
	}

	t.Log(retVal)

	v = c.Cmd("set", "foo", "bar")
	v = c.Cmd("get", "foo")
}