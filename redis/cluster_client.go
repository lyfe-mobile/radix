package redis

import (
		"strconv"
		"math/rand"
		"strings"
		"fmt"
		"io"
	)

type crcTable [256]uint16

const (
	TOTAL_SLOTS = 16384
)

var (
	xmodemTable = &crcTable{0, 4129, 8258, 12387, 16516, 20645, 24774, 28903, 33032, 37161, 41290, 45419, 49548, 53677, 57806, 61935, 4657, 528, 12915, 8786, 21173, 17044, 29431, 25302, 37689, 33560, 45947, 41818, 54205, 50076, 62463, 58334, 9314, 13379, 1056, 5121, 25830, 29895, 17572, 21637, 42346, 46411, 34088, 38153, 58862, 62927, 50604, 54669, 13907, 9842, 5649, 1584, 30423, 26358, 22165, 18100, 46939, 42874, 38681, 34616, 63455, 59390, 55197, 51132, 18628, 22757, 26758, 30887, 2112, 6241, 10242, 14371, 51660, 55789, 59790, 63919, 35144, 39273, 43274, 47403, 23285, 19156, 31415, 27286, 6769, 2640, 14899, 10770, 56317, 52188, 64447, 60318, 39801, 35672, 47931, 43802, 27814, 31879, 19684, 23749, 11298, 15363, 3168, 7233, 60846, 64911, 52716, 56781, 44330, 48395, 36200, 40265, 32407, 28342, 24277, 20212, 15891, 11826, 7761, 3696, 65439, 61374, 57309, 53244, 48923, 44858, 40793, 36728, 37256, 33193, 45514, 41451, 53516, 49453, 61774, 57711, 4224, 161, 12482, 8419, 20484, 16421, 28742, 24679, 33721, 37784, 41979, 46042, 49981, 54044, 58239, 62302, 689, 4752, 8947, 13010, 16949, 21012, 25207, 29270, 46570, 42443, 38312, 34185, 62830, 58703, 54572, 50445, 13538, 9411, 5280, 1153, 29798, 25671, 21540, 17413, 42971, 47098, 34713, 38840, 59231, 63358, 50973, 55100, 9939, 14066, 1681, 5808, 26199, 30326, 17941, 22068, 55628, 51565, 63758, 59695, 39368, 35305, 47498, 43435, 22596, 18533, 30726, 26663, 6336, 2273, 14466, 10403, 52093, 56156, 60223, 64286, 35833, 39896, 43963, 48026, 19061, 23124, 27191, 31254, 2801, 6864, 10931, 14994, 64814, 60687, 56684, 52557, 48554, 44427, 40424, 36297, 31782, 27655, 23652, 19525, 15522, 11395, 7392, 3265, 61215, 65342, 53085, 57212, 44955, 49082, 36825, 40952, 28183, 32310, 20053, 24180, 11923, 16050, 3793, 7920}

)

func checkSum(val []byte, t *crcTable) uint16 {
	var crc uint16
	for _, b := range val {
		crc = ((crc << 8) & 65280) ^ t[((crc >> 8) & 255) ^ uint16(b)]
	}

	return crc & 65535
}

type ClusterRedis struct {
	slotMap [TOTAL_SLOTS]*Shard
	nodeMap map[string]*Client
}

type Shard struct {
	master *Client
	slaves []*Client
	allClis []*Client
}

func (c *ClusterRedis) closeConns() {
	for _, conn := range c.nodeMap {
		if conn != nil {
			conn.Close()
		}
	}

	for i := 0; i < TOTAL_SLOTS; i++ {
		c.slotMap[i] = nil
	}

	c.nodeMap = nil
}

func (c *ClusterRedis) refreshMap() {
	var cli *Client
	for addr, _ := range c.nodeMap{
		var err error
		cli, err = Dial("tcp", addr)
		if err != nil || cli == nil {
			continue
		}
		fmt.Println(cli.conn.RemoteAddr())
		break

	}

	nodeMap := make(map[string]*Client)
	var slotMap [TOTAL_SLOTS]*Shard
	reply := cli.Cmd("cluster", "slots")
	fmt.Println("reply", reply)
	elems := reply.Elems

	for _, e := range elems {
		startSlotReply := e.Elems[0]
		endSlotReply := e.Elems[1]
		startSlot, _ := startSlotReply.Int()
		endSlot, _ := endSlotReply.Int()
		shard := new(Shard)
		for i := 2; i < len(e.Elems); i++ { //
			host := e.Elems[i]
			var hostPort int
			var hostIP string
			hostPort, _ = host.Elems[1].Int()
			hostIP, _ = host.Elems[0].Str()

			hostString := hostIP + ":" + strconv.Itoa(hostPort)
			hostClient, _ := Dial("tcp", hostString)
			nodeMap[hostString] = hostClient
			
			if i == 2 { //master node
				shard.master = hostClient
			} else {
				shard.slaves = append(shard.slaves, hostClient)
			}
			shard.allClis = append(shard.allClis, hostClient)

		}

		for j := startSlot; j <= endSlot; j++ {
			slotMap[j] = shard
		}

	}

	c.closeConns()
	c.nodeMap = nodeMap

	c.slotMap = slotMap
}

func DialCluster(nodes []string) (c *ClusterRedis, err error) {
	c = new(ClusterRedis)
	c.nodeMap = make(map[string]*Client, len(nodes))
	for _, n := range nodes {
		cli, _ := Dial("tcp", n)
		c.nodeMap[n] = cli
	}
	c.refreshMap()

	if err != nil {
		return
	}

	return
}

func computeSlot(key string) int {
	res := checkSum([]byte(key), xmodemTable)
	return int(res) % TOTAL_SLOTS
}


func (c *ClusterRedis) getSlot(key string) (cli *Client) {
	idx := computeSlot(key)
	shard := c.slotMap[idx]
	cliIdx := rand.Intn(len(shard.allClis))
	cli = shard.allClis[cliIdx]
	return cli

}

func (c *ClusterRedis) Cmd(cmd string, args ...interface{}) (reply *Reply) {
	idx := computeSlot(args[0].(string))
	shard := c.slotMap[idx]
	cli := shard.master
	reply = cli.Cmd(cmd, args)
	i := 0
	for reply.Type == ErrorReply && i < len(shard.slaves) {
		_, err := reply.Str()
		if err == io.EOF {
			cli = shard.slaves[i]
			//fmt.Println("slave check", cli.conn.RemoteAddr())
			reply = cli.Cmd(cmd, args)
			i++
		}
			
	}
	
	for reply.Type == MoveReply {
		//fmt.Println(reply)
		c.refreshMap()
		cli := c.getSlot(args[0].(string))
		reply = cli.Cmd(cmd, args)
	} 

	if  reply.Type == AskReply {
		resp, err := reply.Str()
		if err != nil {
			return
		}
		elems := strings.Split(resp, " ")
		cli = c.nodeMap[elems[2]]
		cli.Cmd("ASKING")
		reply = cli.Cmd(cmd, args)		


	}

	return
}