package public

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"os"
	. "redis_performance_analysis/big_key/dump"
	. "redis_performance_analysis/hot_key"
)

var (
	BigKey               bool   // enable big key analysis
	HotKey               bool   // enable hot key analysis
	PathAddr             string // path to addr
	KeyTop               uint   // key top number
	MonitorTime          uint   // hotkey monitor time
	MonitorPort          uint   // hotkey monitor port
	MonitorIp            string // hotkey monitor ip
	MonitorDevice        string // hotkey monitor device
	MaxKeyLength         uint   // show hot key max key length
	AnalysisThreadNumber uint32 // analysis thread number
	WriteFile            bool   // hot key write file
	Version              bool   // show version info
	Help                 bool   // show help info
	OfflineMode          bool   // offline mode
)

func Run() {
	pflag.BoolVarP(&BigKey, "big-key", "b", false, "enable big key analysis")
	pflag.BoolVarP(&HotKey, "hot-key", "h", false, "enable hot key analysis")
	pflag.StringVarP(&PathAddr, "path", "p", "", "path to addr")
	pflag.UintVarP(&KeyTop, "key-top", "k", 100, "key top number")
	pflag.UintVarP(&MonitorTime, "monitor-time", "m", 10, "hotkey monitor time")
	pflag.UintVarP(&MaxKeyLength, "max-key-length", "l", 100, "show hot key max key length")
	pflag.Uint32VarP(&AnalysisThreadNumber, "thread-number", "t", 5, "analysis thread number")
	pflag.BoolVarP(&WriteFile, "write-file", "w", false, "hot key write file")
	pflag.StringVarP(&MonitorDevice, "device", "d", "", "hotkey monitor device")
	pflag.StringVarP(&MonitorIp, "ip", "i", "", "hotkey monitor ip")
	pflag.UintVarP(&MonitorPort, "port", "s", 0, "hotkey monitor port")
	pflag.BoolVarP(&Version, "version", "v", false, "show version info")
	pflag.BoolVarP(&OfflineMode, "offline-mode", "o", false, "offline mode")
	pflag.BoolVar(&Help, "help", false, "show help info")
	pflag.Parse()

	if Version {
		PrintVersion()
		os.Exit(0)
	}
	if Help || (!BigKey && !HotKey) {
		PrintHelp(os.Args[0])
		os.Exit(0)
	}
	if BigKey && PathAddr == "" {
		log.Warnf("big key analysis requires path to addr")
		PathAddr, _ = os.Getwd()
	}
	if BigKey {
		LoadBigKey()
	}
	if HotKey {
		LoadHotKey()
	}
}

func LoadBigKey() {
	addr := readFileName(PathAddr, ".rdb")
	for _, a := range addr {
		data, err := Show(context.Background(), a)
		if err != nil {
			log.Errorf("show rdb file %s fail, err: %v", a, err)
			continue
		}
		fmt.Println(data)
	}
}

func LoadHotKey() {
	if MonitorPort == 0 {
		log.Errorf("hot key analysis requires port")
		return
	}
	var data map[string]interface{}
	var err error
	if OfflineMode {
		addr := readFileName(PathAddr, ".pcap")
		for _, a := range addr {
			var pcapFile *os.File
			pcapFile, err = os.Open(a)
			if err != nil {
				log.Errorf("open pcap file %s fail, err: %v", a, err)
				continue
			}
			data, err = ShowHotKeys(context.Background(), "", int(MonitorTime), int(MonitorPort), int(MaxKeyLength),
				int(KeyTop), pcapFile, WriteFile, MonitorIp, AnalysisThreadNumber)
			if err != nil {
				log.Errorf("show hot key file %s fail, err: %v", a, err)
				continue
			}
			fmt.Println(data)
		}
	} else {
		data, err = ShowHotKeys(context.Background(), MonitorDevice, int(MonitorTime), int(MonitorPort), int(MaxKeyLength),
			int(KeyTop), nil, WriteFile, MonitorIp, AnalysisThreadNumber)
		if err != nil {
			log.Errorf("show hot key fail, err: %v", err)
			return
		}
		fmt.Println(data)
	}
}
