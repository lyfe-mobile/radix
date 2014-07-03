package redis

import (
		"github.com/howeyc/crc16"
		"fmt"
		"io"
		"strings"
		//"encoding/hex"
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
	c.nodeMap = make(map[string]*Client, len(nodes))
	cli, err := Dial("tcp", nodes[0])
	if err != nil {
		return
	}

	fmt.Println(cli.Cmd("cluster", "slots").List())
	//j := 0
	/*
	for _, n := range nodes {
		cli, err := Dial("tcp", n)
		c.nodeMap[n] = cli
		if err != nil {
			return c, err
		}
		for i := 0; i < slotsPerNode; i++ {
			c.slotMap[j] = cli
			j++
		}
	}

	for l := j; l < TOTAL_SLOTS; l++ {
		idx := l % len(nodes)
		c.slotMap[l] = c.slotMap[idx]
	}
	*/

	return
}

func computeSlot(key string) int {
	tab := crc16.MakeTable(1021)
	res := crc16.Checksum([]byte(key), tab)
	h := crc16.New(tab)
	io.WriteString(h, "123456789")
	res2 := h.Sum(nil)
	fmt.Println(res2)
	return int(res) % TOTAL_SLOTS
}

func (c *ClusterRedis) setSlot(idx int, cli *Client) {
	c.slotMap[idx] = cli
}

func (c *ClusterRedis) getSlot(key string) (cli *Client) {
	idx := computeSlot(key)
	println(idx)
	return c.slotMap[idx]
}

func (c *ClusterRedis) Cmd(cmd string, args ...interface{}) (reply *Reply) {
	cli := c.getSlot(args[0].(string))
	reply = cli.Cmd(cmd, args)
	if reply.Type == MoveReply {
		repString := string(reply.buf)
		nodeInfo := strings.Split(repString, " ")
		fmt.Println(nodeInfo[2], c.nodeMap)
		correctNode := nodeInfo[2]
		fmt.Println(c.nodeMap[correctNode])
		correctCli := c.nodeMap[correctNode]
		reply = correctCli.Cmd(cmd, args)
	}
	return
}