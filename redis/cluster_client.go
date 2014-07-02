package redis

import (
		"github.com/howeyc/crc16"
	)

const (
	TOTAL_SLOTS = 16384
)

type ClusterRedis struct {
	nodeMap map[string]*Client
	slotMap [TOTAL_SLOTS]*Client
}


func DialCluster(nodes []string) (c *ClusterRedis, err error) {
	c = new(ClusterRedis)
	slotsPerNode := TOTAL_SLOTS / len(nodes)
	slotsLeftover := TOTAL_SLOTS - (slotsPerNode * len(nodes))
	println(slotsLeftover)
	j := 0

	for _, n := range nodes {
		cli, err := Dial("tcp", n)
		if err != nil {
			return c, err
		}
		for i := 0; i < slotsPerNode; i++ {
			println(j)
			c.slotMap[j] = cli
			j++
		}
	}

	for l := j; l < TOTAL_SLOTS; l++ {
		println(l)
		idx := l % len(nodes)
		c.slotMap[l] = c.slotMap[idx]
	}

	return
}

func computeSlot(key string) int {
	tab := crc16.MakeTable(0)
	res := crc16.Checksum([]byte(key), tab)
	return int(res) % TOTAL_SLOTS
}

func (c *ClusterRedis) setSlot(idx int, cli *Client) {
	c.slotMap[idx] = cli
}

func (c *ClusterRedis) getSlot(key string) (cli *Client) {
	idx := computeSlot(key)
	return c.slotMap[idx]
}

func (c *ClusterRedis) Cmd(cmd string, args ...interface{}) (reply *Reply) {
	cli := c.getSlot(args[0].(string))
	reply = cli.Cmd(cmd, args)
	return
}