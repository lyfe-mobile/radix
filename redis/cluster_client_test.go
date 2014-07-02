package redis
import (
		"github.com/fzzy/radix/redis"
		"testing"
		
	)

func TestCmd(t *testing.T) {
	nodes := []string{"localhost:7000", "localhost:7001", "localhost:7002" , "localhost:7003", "localhost:7004", "localhost:7005"}
	c, _ := redis.DialCluster(nodes)
	v := c.Cmd("set", "Hello", "World!")
	v = c.Cmd("get", "Hello")
	t.Log(v)
}