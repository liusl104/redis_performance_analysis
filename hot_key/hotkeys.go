package hotkeys

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	log "github.com/sirupsen/logrus"
	"hash/fnv"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	snapshotLen int32 = 65535
	timeout           = 30 * time.Second
	separators        = ":;,_- "
)

type OverallStats struct {
	// 概览

	ActiveProcessed  uint64  `json:"active_processed"`  // 在线活跃线程数
	TotalAccessSum   int64   `json:"total_sum"`         // 总访问次数
	TotalAccessTime  int64   `json:"total_access_time"` // 总访问时间，Microsecond 微妙
	CommandsSec      float64 `json:"commands_sec"`      // 平均每秒访问次数
	TopPrefixes      []*KV   `json:"top_prefixes"`      // 前缀访问次数最多的
	TopKeys          []*KV   `json:"top_keys"`          // top keys 使用最多的key
	TopCommands      []*KV   `json:"top_commands"`      // 使用最多的命令。 key 次数
	HeaviestCommands []*KV   `json:"heaviest_commands"` // 命令类型耗时 Microsecond 微妙
	SlowestCalls     []*KV   `json:"slowest_calls"`     // 慢命令top
	IPV4Call         []*KV   `json:"ipv4_call"`         // IP 访问次数分布 top 10
	CommandTimes
	Other
	tmpTopKeys map[string]int64
}

type CommandTimes struct {
	// 请求响应平均耗时
	Median int64 `json:"median"` // 平均耗时
	P999   int64 `json:"p_999"`  // p999 平均耗时
	P99    int64 `json:"p_99"`   // p99 平均耗时
	P95    int64 `json:"p_95"`   // p95 平均耗时
	P90    int64 `json:"p_90"`   // p90 平均耗时
	P85    int64 `json:"p_85"`   // p85 平均耗时
	P80    int64 `json:"p_80"`   // p90 平均耗时
	P75    int64 `json:"p_75"`   // p75 平均耗时
}

type Other struct {
	PacketSum        int64 `json:"packet_sum"`         // 总计算包数量
	NewConnectNum    int   `json:"new_connect_num"`    // 新建连接数
	CloseConnectNum  int64 `json:"close_connect_num"`  // 连接断开数
	DiscardPacketSum int64 `json:"discard_packet_sum"` // 丢弃包数量
	MonitorStartTime int64 `json:"monitor_start_time"` // 监控开始时间，时间戳，微秒
	MonitorEndTime   int64 `json:"monitor_end_time"`   // 监控结束时间，时间戳，微秒
}

type KV struct {
	Key   string `json:"key"`
	Value int64  `json:"value"`
}
type NetPacket struct {
	PacketContent gopacket.Packet
	ReceiveTime   int64
}
type FullKey struct {
	FullCmd     string
	ReceiveTime int64
	Src         string
	Dst         string
}

type link struct {
	src          string
	dst          string
	transmission chan *NetPacket
	stat         *OverallStats
}

var activeConnection []string

func ShowHotKeys(ctx context.Context, device string, mTime, dPort, cmdLen, top int, pcapFile *os.File, hotKeyWrite bool, hostIp string, threadNum uint32) (map[string]interface{}, error) {
	overallStat := newOverallStats()
	var handle *pcap.Handle
	var err error
	if pcapFile != nil {
		handle, err = pcap.OpenOfflineFile(pcapFile)
	} else {
		handle, err = pcap.OpenLive(device, snapshotLen, false, timeout)
	}

	if err != nil {
		return nil, err
	}
	defer func() {
		go handle.Close()
	}()
	endTime := time.Now().UnixMicro()
	overallStat.MonitorStartTime = time.Now().UnixMicro()

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	var resourceAllocation map[int]*link = make(map[int]*link)
	// 初始化资源
	for i := 0; i < int(threadNum); i++ {
		resourceAllocation[i] = &link{
			src:          "",
			dst:          "",
			transmission: make(chan *NetPacket, 10000000),
			stat:         newOverallStats(),
		}
	}
	timeOut := time.After(time.Second * time.Duration(mTime))
	// var limitNetworkPackageSize int = 1024 << 20
	go func() {
		log.Infof("开始接收网络消息")
		var networkIp string
		var hashKey int
		var netLayer gopacket.NetworkLayer
		for {
			select {
			case <-timeOut:
				log.Infof("接收网络消息结束")
				for _, v := range resourceAllocation {
					close(v.transmission)
				}
				log.Infof("结束资源通道")
				endTime = time.Now().UnixMicro()
				return
			case <-ctx.Done():
				// cc()
				log.Infof("取消资源通道")
				return
			case packet := <-packetSource.Packets():
				data := &NetPacket{
					PacketContent: packet,
					ReceiveTime:   time.Now().UnixMicro(),
				}
				netLayer = data.PacketContent.NetworkLayer()
				if netLayer != nil {
					if strings.Contains(netLayer.NetworkFlow().Dst().String(), hostIp) {
						networkIp = netLayer.NetworkFlow().Src().String()
						hashKey = hashToBucket(networkIp, threadNum)
						resourceAllocation[hashKey].transmission <- data
						//	netLayer.NetworkFlow().
					}
					if strings.Contains(netLayer.NetworkFlow().Src().String(), hostIp) {
						networkIp = netLayer.NetworkFlow().Dst().String()
						hashKey = hashToBucket(networkIp, threadNum)
						resourceAllocation[hashKey].transmission <- data
					}
				} else {
					overallStat.Other.PacketSum++
				}

			}
		}

	}()

	var cmdFile *os.File
	var bufferWrite *bufio.Writer
	if hotKeyWrite {
		log.Infof("开始写入文件")
		cmdFile, err = os.OpenFile(fmt.Sprintf("/tmp/%d_%d.txt", overallStat.MonitorStartTime, dPort), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		// 写入缓存
		bufferWrite = bufio.NewWriter(cmdFile)
		defer func() {
			err = bufferWrite.Flush()
			err = cmdFile.Close()
		}()
	}

	var wg *sync.WaitGroup = &sync.WaitGroup{}
	for threadId, resource := range resourceAllocation {
		wg.Add(1)
		log.Infof("开始%d线程", threadId)
		go func(allocate *link, threadId int) {
			defer wg.Done()
			var timeDiff = make(map[string]map[string]int64)
			for {
				select {
				case <-ctx.Done():
					// cc()
					log.Infof("取消%d线程", threadId)
					return
				case packet := <-allocate.transmission:
					if packet == nil {
						log.Infof("结束%d线程", threadId)
						return
					}
					PacketInfo(packet, dPort, hostIp, cmdLen, allocate.stat, bufferWrite, timeDiff)
				}
			}
		}(resource, threadId)
	}
	log.Infof("等待处理线程结束")
	wg.Wait()
	log.Infof("开始聚合数据")
	overallStat.MonitorEndTime = endTime
	aggregation(overallStat, resourceAllocation)
	analysisResult := analysisCounter(overallStat, top)
	log.Infof("分析数据结束")
	return analysisResult, nil
}

func analysisCounter(overallStat *OverallStats, topNum int) map[string]interface{} {
	// 结束监听
	// overallStat.MonitorEndTime = time.Now().UnixMicro()
	// 排序
	log.Infof("计算排序")
	sort.Slice(overallStat.TopPrefixes, func(i, j int) bool { return overallStat.TopPrefixes[i].Value > overallStat.TopPrefixes[j].Value })
	for key, value := range overallStat.tmpTopKeys {
		if len(overallStat.TopKeys) <= topNum {
			overallStat.TopKeys = append(overallStat.TopKeys, &KV{
				Key:   key,
				Value: value,
			})
		} else {
			overallStat.TopKeys = append(overallStat.TopKeys, &KV{
				Key:   key,
				Value: value,
			})
			sort.Slice(overallStat.TopKeys, func(i, j int) bool { return overallStat.TopKeys[i].Value < overallStat.TopKeys[j].Value })
			overallStat.TopKeys = overallStat.TopKeys[1:]
		}
	}
	sort.Slice(overallStat.TopKeys, func(i, j int) bool { return overallStat.TopKeys[i].Value > overallStat.TopKeys[j].Value })
	sort.Slice(overallStat.TopCommands, func(i, j int) bool { return overallStat.TopCommands[i].Value > overallStat.TopCommands[j].Value })
	sort.Slice(overallStat.HeaviestCommands, func(i, j int) bool {
		return overallStat.HeaviestCommands[i].Value > overallStat.HeaviestCommands[j].Value
	})
	sort.Slice(overallStat.IPV4Call, func(i, j int) bool { return overallStat.IPV4Call[i].Value > overallStat.IPV4Call[j].Value })

	// 由小到大排序，计算P95，P99等值
	log.Infof("计算P值")
	sort.Slice(overallStat.SlowestCalls, func(i, j int) bool { return overallStat.SlowestCalls[i].Value < overallStat.SlowestCalls[j].Value })
	if len(overallStat.SlowestCalls) > 0 {
		overallStat.P999 = overallStat.SlowestCalls[int(float64(len(overallStat.SlowestCalls))*0.999)].Value
		overallStat.P99 = overallStat.SlowestCalls[int(float64(len(overallStat.SlowestCalls))*0.99)].Value
		overallStat.P95 = overallStat.SlowestCalls[int(float64(len(overallStat.SlowestCalls))*0.95)].Value
		overallStat.P90 = overallStat.SlowestCalls[int(float64(len(overallStat.SlowestCalls))*0.9)].Value
		overallStat.P85 = overallStat.SlowestCalls[int(float64(len(overallStat.SlowestCalls))*0.85)].Value
		overallStat.P80 = overallStat.SlowestCalls[int(float64(len(overallStat.SlowestCalls))*0.80)].Value
		overallStat.P75 = overallStat.SlowestCalls[int(float64(len(overallStat.SlowestCalls))*0.75)].Value
		overallStat.Median = overallStat.SlowestCalls[int(float64(len(overallStat.SlowestCalls))/2)].Value
	}
	// 由大到小排序
	log.Infof("计算Slow")
	sort.Slice(overallStat.SlowestCalls, func(i, j int) bool { return overallStat.SlowestCalls[i].Value > overallStat.SlowestCalls[j].Value })

	if len(overallStat.TopPrefixes) > topNum {
		overallStat.TopPrefixes = overallStat.TopPrefixes[:topNum]
	}
	/*if len(overallStat.TopKeys) > topNum {
		overallStat.TopKeys = overallStat.TopKeys[:topNum]
	}*/
	if len(overallStat.SlowestCalls) > topNum {
		overallStat.SlowestCalls = overallStat.SlowestCalls[:topNum]
	}
	if len(overallStat.IPV4Call) > topNum {
		overallStat.IPV4Call = overallStat.IPV4Call[:topNum]
	}

	// 每秒执行命令数量
	log.Infof("计算每秒速度")
	overallStat.CommandsSec = Decimal(float64(overallStat.TotalAccessSum) / float64((overallStat.MonitorEndTime-overallStat.MonitorStartTime)/1000/1000))
	log.Infof("解析json")
	m := Struct2MapByTag(overallStat, "json")
	return m
}

// PacketInfo 接收网络包分析
/*
func PacketInfo(packet gopacket.Packet, dPort int, cmdLen int) {
	tcpLayer := packet.Layer(layers.LayerTypeTCP)
	if tcpLayer != nil {
		tcp, _ := tcpLayer.(*layers.TCP)
		if tcp.DstPort == layers.TCPPort(dPort) {
			applicationLayer := packet.ApplicationLayer()
			if applicationLayer != nil {
				str := string(applicationLayer.Payload()[:])
				arr := Split(str, "\r\n")
				if len(arr) >= 5 {
					cmd := arr[2]
					key := arr[4]
					l := len(key)
					if l >= cmdLen {
						l = cmdLen
					}
					redisCmd := cmd + " " + key[:l]
					if v, ok := counter[redisCmd]; ok {
						counter[redisCmd] = v + 1
					} else {
						counter[redisCmd] = 1
					}
				}
			}
		}
	}
}
*/
func PacketInfo(packet *NetPacket, dPort int, hostIp string, cmdLen int, stat *OverallStats, cmdFile *bufio.Writer, timeDiff map[string]map[string]int64) {
	stat.PacketSum++
	packet.ReceiveTime = packet.PacketContent.Metadata().Timestamp.UnixMicro()
	tcpLayer := packet.PacketContent.Layer(layers.LayerTypeTCP)
	netLayer := packet.PacketContent.NetworkLayer()
	var execTime int64
	if tcpLayer != nil {
		tcp, _ := tcpLayer.(*layers.TCP)
		log.Debugf("FIN %v, SYN %v, RST %v, PSH %v, ACK %v, URG %v, ECE %v, CWR %v, NS %v ", tcp.FIN, tcp.SYN, tcp.RST, tcp.PSH, tcp.ACK, tcp.URG, tcp.ECE, tcp.CWR, tcp.NS)
		if tcp.DstPort == layers.TCPPort(dPort) || tcp.SrcPort == layers.TCPPort(dPort) {

			Src := netLayer.NetworkFlow().Src().String()
			Dst := netLayer.NetworkFlow().Dst().String()
			if tcp.DstPort == layers.TCPPort(dPort) {
				if !slices.Contains(activeConnection, fmt.Sprintf("%s:%s", Src, tcp.SrcPort.String())) {
					activeConnection = append(activeConnection, fmt.Sprintf("%s:%s", Src, tcp.SrcPort.String()))
					stat.ActiveProcessed++
				}
			}

			applicationLayer := packet.PacketContent.ApplicationLayer()
			switch {
			case tcp.FIN: // 结束连接
				stat.CloseConnectNum += 1
			case tcp.SYN: // 建立连接
				stat.NewConnectNum += 1
			case tcp.RST: // 连接重置
				log.Debugf("RST Src: %s:%s Dst: %s:%s ", Src, tcp.SrcPort.String(), Dst, tcp.DstPort.String())
			case tcp.PSH && tcp.ACK: // 数据传输
				log.Debugf("PSH+ACK Src: %s:%s Dst: %s:%s ", Src, tcp.SrcPort.String(), Dst, tcp.DstPort.String())
				if tcp.SrcPort == layers.TCPPort(dPort) {
					uniqueIdentification := fmt.Sprintf("%s:%s-%s:%s-%d", Dst, tcp.DstPort.String(), Src, tcp.SrcPort.String(), tcp.Seq)
					if v, ok := timeDiff[uniqueIdentification]; ok {
						for key, value := range v {
							redisCmd := strings.Split(key, " ")
							cmd := redisCmd[0]
							if strings.HasPrefix(strings.ToLower(cmd), "auth") {
								// 认证命令不记录
								continue
							}
							execTime = packet.ReceiveTime - value
							stat.TotalAccessTime += execTime
							// redisKey := strings.Join(redisCmd[1:], " ")
							stat.SlowestCalls = addKv(stat.SlowestCalls, key, execTime)
							if foundKv(stat.HeaviestCommands, cmd) {
								modifyKv(stat.HeaviestCommands, cmd, execTime)
							} else {
								stat.HeaviestCommands = addKv(stat.HeaviestCommands, cmd, execTime)
							}

						}

					} else {
						stat.DiscardPacketSum++
					}
				}

			case tcp.PSH:
				log.Debugf("PSH Src: %s:%s Dst: %s:%s ", Src, tcp.SrcPort.String(), Dst, tcp.DstPort.String())
			case tcp.URG:
				log.Debugf("URG Src: %s:%s Dst: %s:%s ", Src, tcp.SrcPort.String(), Dst, tcp.DstPort.String())
			case tcp.ECE:
				log.Debugf("ECE Src: %s:%s Dst: %s:%s ", Src, tcp.SrcPort.String(), Dst, tcp.DstPort.String())
			case tcp.CWR:
				log.Debugf("CWR Src: %s:%s Dst: %s:%s ", Src, tcp.SrcPort.String(), Dst, tcp.DstPort.String())
			case tcp.NS:
				log.Debugf("NS Src: %s:%s Dst: %s:%s ", Src, tcp.SrcPort.String(), Dst, tcp.DstPort.String())
			case tcp.ACK: // 连接响应
				log.Debugf("ACK  Src: %s:%s Dst: %s:%s ", Src, tcp.SrcPort.String(), Dst, tcp.DstPort.String())
			default:
				log.Debugf("Src: %s:%s Dst: %s:%s ", Src, tcp.SrcPort.String(), Dst, tcp.DstPort.String())
			}

			if applicationLayer != nil {
				if tcp.DstPort == layers.TCPPort(dPort) && Dst == hostIp {
					str := string(applicationLayer.Payload()[:])
					arr := Split(str, "\r\n")
					// 获取访问命令
					var cmd string
					var fullCmd []string
					if len(arr) < 3 {
						log.Warnf("skip command : %s", strings.Join(arr, " "))
						return
					}
					cmd = arr[2]
					// 需要去除认证
					if len(arr) >= 5 && !strings.HasPrefix(strings.ToLower(cmd), "auth") {
						for _, key := range arr {
							switch {
							case strings.HasPrefix(key, "$"):
								continue
							case strings.HasPrefix(key, "*"):
								continue
							case key == "":
								continue
							default:
								fullCmd = append(fullCmd, key)
							}
						}
						if cmdFile != nil {
							cmdBuf := new(FullKey)
							cmdBuf.FullCmd = strings.Join(fullCmd, " ")
							cmdBuf.Src = fmt.Sprintf("%s:%s", Src, tcp.SrcPort.String())
							cmdBuf.Dst = fmt.Sprintf("%s:%s", Dst, tcp.DstPort.String())
							cmdBuf.ReceiveTime = packet.ReceiveTime
							tmpStr, err := json.Marshal(&cmdBuf)
							if err != nil {
								log.Warnf("%v", err)
							} else {
								_, err = cmdFile.Write(tmpStr)
								err = cmdFile.WriteByte('\n')
								if err != nil {
									log.Warnf("wirte file fail:%v", err)
								}
							}
						}

						// 统计访问总次数
						stat.TotalAccessSum += 1
						// 获取访问key
						var key string
						if len(fullCmd) >= 2 {
							for i := 1; i < len(fullCmd); i++ {
								if fullCmd[i] == " " || len(fullCmd[i]) <= 2 {
									continue
								}
								key = fullCmd[i]
								break
							}

						}

						l := len(key)
						// 判断key长度
						if l >= cmdLen {
							l = cmdLen
						}
						// 收集key访问次数
						redisCmd := cmd + " " + key[:l]
						if _, ok := stat.tmpTopKeys[redisCmd]; ok {
							stat.tmpTopKeys[redisCmd] += 1
						} else {
							stat.tmpTopKeys[redisCmd] = 1
						}
						/*if foundKv(stat.TopKeys, redisCmd) {
							modifyKv(stat.TopKeys, redisCmd, 1)
						} else {
							stat.TopKeys = addKv(stat.TopKeys, redisCmd, 1)
						}*/

						// 收集访问key类型,范围次数
						if foundKv(stat.TopCommands, cmd) {
							modifyKv(stat.TopCommands, cmd, 1)
						} else {
							stat.TopCommands = addKv(stat.TopCommands, cmd, 1)
						}

						// 收集IP信息
						if foundKv(stat.IPV4Call, Src) {
							modifyKv(stat.IPV4Call, Src, 1)
						} else {
							stat.IPV4Call = addKv(stat.IPV4Call, Src, 1)
						}

						// 记录接受信息
						uniqueIdentification := fmt.Sprintf("%s:%s-%s:%s-%d", Src, tcp.SrcPort.String(), Dst, tcp.DstPort.String(), tcp.Ack)
						log.Debugf("记录key：%s ", uniqueIdentification)
						if _, ok := timeDiff[uniqueIdentification]; ok {
							log.Debugf("key存在 %s", uniqueIdentification)
						} else {
							timeDiff[uniqueIdentification] = map[string]int64{
								redisCmd: packet.ReceiveTime,
							}
						}
						// 收集前缀key
						prefixes := getPrefixes(key, separators)
						for _, prefix := range prefixes {
							if len(prefix) == 0 {
								continue
							}
							if foundKv(stat.TopPrefixes, prefix) {
								modifyKv(stat.TopPrefixes, prefix, 1)
							} else {
								stat.TopPrefixes = addKv(stat.TopPrefixes, prefix, 1)
							}
						}
					}
				}
			}
		}
	}
	// 取10000条采样
	/*sort.Slice(stat.TopPrefixes, func(i, j int) bool { return stat.TopPrefixes[i].Value > stat.TopPrefixes[j].Value })
	sort.Slice(stat.TopKeys, func(i, j int) bool { return stat.TopKeys[i].Value > stat.TopKeys[j].Value })
	sort.Slice(stat.TopCommands, func(i, j int) bool { return stat.TopCommands[i].Value > stat.TopCommands[j].Value })
	sort.Slice(stat.HeaviestCommands, func(i, j int) bool {
		return stat.HeaviestCommands[i].Value > stat.HeaviestCommands[j].Value
	})
	sort.Slice(stat.IPV4Call, func(i, j int) bool { return stat.IPV4Call[i].Value > stat.IPV4Call[j].Value })*/
	sort.Slice(stat.SlowestCalls, func(i, j int) bool { return stat.SlowestCalls[i].Value < stat.SlowestCalls[j].Value })
	if len(stat.SlowestCalls) >= 1000 {
		stat.SlowestCalls = stat.SlowestCalls[1:]
	}
}

func aggregation(stat *OverallStats, newStat map[int]*link) {
	for i, l := range newStat {
		log.Infof("第%d个统计周期", i)
		stat.TotalAccessSum += l.stat.TotalAccessSum
		stat.TotalAccessTime += l.stat.TotalAccessTime
		stat.ActiveProcessed += l.stat.ActiveProcessed
		stat.DiscardPacketSum += l.stat.DiscardPacketSum
		for _, value := range l.stat.IPV4Call {
			if foundKv(stat.IPV4Call, value.Key) {
				modifyKv(stat.IPV4Call, value.Key, value.Value)
			} else {
				stat.IPV4Call = append(stat.IPV4Call, value)
			}
		}
		log.Infof("number: %d IPV4Call", i)
		for _, value := range l.stat.TopPrefixes {
			if foundKv(stat.TopPrefixes, value.Key) {
				modifyKv(stat.TopPrefixes, value.Key, value.Value)
			} else {
				stat.TopPrefixes = append(stat.TopPrefixes, value)
			}
		}
		log.Infof("number: %d TopCommands", i)
		for _, value := range l.stat.TopCommands {
			if foundKv(stat.TopCommands, value.Key) {
				modifyKv(stat.TopCommands, value.Key, value.Value)
			} else {
				stat.TopCommands = append(stat.TopCommands, value)
			}
		}
		log.Infof("number: %d TopKeys", i)
		/*for _, value := range l.stat.TopKeys {
			if foundKv(stat.TopKeys, value.Key) {
				modifyKv(stat.TopKeys, value.Key, value.Value)
			} else {
				stat.TopKeys = append(stat.TopKeys, value)
			}
		}*/
		for key, value := range l.stat.tmpTopKeys {
			if _, ok := stat.tmpTopKeys[key]; ok {
				stat.tmpTopKeys[key] += value
			} else {
				stat.tmpTopKeys[key] = value
			}
		}
		l.stat.tmpTopKeys = nil
		log.Infof("number: %d SlowestCalls", i)
		for _, value := range l.stat.SlowestCalls {
			stat.SlowestCalls = append(stat.SlowestCalls, value)
		}
		log.Infof("number: %d HeaviestCommands", i)
		for _, value := range l.stat.HeaviestCommands {
			if foundKv(stat.HeaviestCommands, value.Key) {
				modifyKv(stat.HeaviestCommands, value.Key, value.Value)
			} else {
				stat.HeaviestCommands = append(stat.HeaviestCommands, value)
			}
		}
		stat.CloseConnectNum += l.stat.CloseConnectNum
		stat.NewConnectNum += l.stat.NewConnectNum
		stat.PacketSum += l.stat.PacketSum
		stat.CloseConnectNum += l.stat.CloseConnectNum
	}
}

func newOverallStats() *OverallStats {
	return &OverallStats{
		TopPrefixes:      []*KV{},
		TopKeys:          []*KV{},
		TopCommands:      []*KV{},
		HeaviestCommands: []*KV{},
		SlowestCalls:     []*KV{},
		IPV4Call:         []*KV{},
		tmpTopKeys:       map[string]int64{},
	}
}

func hashToBucket(s string, threadNumber uint32) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() % threadNumber)
}
