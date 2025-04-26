package public

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"os"
	"path"
	"strings"
)

var (
	VersionName = "v1.0.0"
	GitHash     string
	GitBranch   string
	GoVersion   string
	BuildTS     string
)

// readRdbFileName reads the RDB file name from the given path.
func readFileName(inPath string, fileType string) []string {
	var fileList []string
	if inPath == "" {
		log.Errorf("inPath is empty")
		return fileList
	}
	if _, err := os.Stat(inPath); os.IsNotExist(err) {
		log.Errorf("inPath is not exist")
		return fileList
	}
	fileInfo, _ := os.Stat(inPath)
	if fileInfo.IsDir() {
		entries, _ := os.ReadDir(inPath)
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if strings.HasSuffix(entry.Name(), fileType) {
				fileList = append(fileList, path.Join(inPath+"/"+entry.Name()))
			}
		}
		if len(fileList) == 0 {
			log.Errorf("not found %s file", fileType)
		}
	} else {
		fileList = append(fileList, inPath)
	}

	return fileList
}

// readNetDriveName reads the network drive name from the given IP address.
func readNetDriveName(ip string) string {
	return ""
}

func PrintVersion() {
	var info string
	info += fmt.Sprintf("Git Commit Hash: %s\n", GitHash)
	info += fmt.Sprintf("Git Branch: %s - %s\n", GitBranch, VersionName)
	info += fmt.Sprintf("Build Time: %s\n", BuildTS)
	info += fmt.Sprintf("Go Version: %s", GoVersion)
	fmt.Println(info)
}

func PrintHelp(appName string) {
	fmt.Printf("Usage: %s [type] [OPTIONS]\n", appName)
	pflag.PrintDefaults()
	os.Exit(0)
}
