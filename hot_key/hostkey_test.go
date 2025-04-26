package hotkeys

import (
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"os"
	"strings"
	"testing"
)

func TestShowHotKeys(t *testing.T) {

	pcapFile, err := os.Open("/root/7775.pcap")
	if err != nil {
		t.Fatal(err)
	}
	info, err := ShowHotKeys(nil, "", 10, 7775, 200, 10, pcapFile, false, "10.192.102.3", 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(info)
}

func TestShowHotLive(t *testing.T) {
	handle, err := pcap.OpenLive("en0", snapshotLen, false, timeout)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	var timeDiff = make(map[string]map[string]int)
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	for packet := range packetSource.Packets() {
		tcpLayer := packet.Layer(layers.LayerTypeTCP)
		netLayer := packet.NetworkLayer()
		if tcpLayer != nil {
			tcp, _ := tcpLayer.(*layers.TCP)
			Src := netLayer.NetworkFlow().Src().String()
			Dst := netLayer.NetworkFlow().Dst().String()
			DstPort := tcp.DstPort.String()
			SrcPort := tcp.SrcPort.String()
			applicationLayer := packet.ApplicationLayer()

			if strings.Contains(DstPort, "6379") || strings.Contains(SrcPort, "6379") {
				Src = netLayer.NetworkFlow().Src().String()
				Dst = netLayer.NetworkFlow().Dst().String()
				switch {
				case tcp.FIN: // 结束连接

				case tcp.SYN: // 建立连接

				case tcp.RST: // 连接重置
				case tcp.PSH && tcp.ACK: // 数据传输
					if tcp.SrcPort == layers.TCPPort(6379) {
						uniqueIdentification := fmt.Sprintf("%s:%s-%s:%s-%d", Dst, tcp.DstPort.String(), Src, tcp.SrcPort.String(), tcp.Seq)
						if v, ok := timeDiff[uniqueIdentification]; ok {
							for key, value := range v {
								t.Logf("%s  %d ", key, value)
							}
						}
					}
				}
			}
			if applicationLayer != nil {
				if tcp.DstPort == layers.TCPPort(6379) {
					str := string(applicationLayer.Payload()[:])
					arr := Split(str, "\r\n")
					// 获取访问命令
					// var cmd string
					var fullcmd string
					if len(arr) < 3 {
						return
					}

					for _, key := range arr {
						switch {
						case strings.HasPrefix(key, "$"):
							fullcmd += " "
						case strings.HasPrefix(key, "*"):
							continue
						case key == "":
							continue
						default:
							fullcmd += key
						}
					}
					t.Log(fullcmd)

				}
			}
		}
	}
}
