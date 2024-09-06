package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/rolandhe/smss-client/client"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	host = flag.String("host", "", "master host")
	port = flag.Int("port", 12301, "master port")
)

func main() {
	flag.Parse()
	reader := bufio.NewReader(os.Stdin)
	for {
		prompt()
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}
		if len(line) == 0 {
			continue
		}
		line = strings.TrimSpace(line)
		if line == "list" {
			list()
			continue
		}
		if strings.HasPrefix(line, "delete ") {
			name := strings.TrimSpace(line[len("delete "):])
			deleteTopic(name)
			continue
		}
		if strings.HasPrefix(line, "create ") {
			createTopic(line)
			continue
		}
		if line == "help" {
			doHelp()
			continue
		}
		if line == "quit" {
			return
		}
	}
}

func prompt() {
	fmt.Print(">")
}

func doHelp() {
	fmt.Println("   list: list all topic info,output json")
	fmt.Println("   create topicName lifetime: create topic")
	fmt.Println("        -- lifetime is optional, default 0, unit second")
	fmt.Println("        -- 0 indicates unlimited")
	fmt.Println("   delete topicName: delete topic")
	fmt.Println("   quit: quit command line")
}

func createTopic(line string) {
	line = strings.TrimSpace(line[len("create "):])
	items := strings.Split(line, " ")
	topic := strings.TrimSpace(items[0])
	life := int64(0)
	var err error
	if len(items) > 1 {
		sLife := strings.TrimSpace(items[1])
		delay, err := strconv.ParseInt(sLife, 10, 64)
		if err != nil {
			fmt.Println(err)
			return
		}
		if delay > 0 {
			life = time.Now().UnixMilli() + delay*1000
		}
	}
	pub, err := client.NewPubClient(*host, *port, time.Second*120)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer pub.Close()

	err = pub.CreateTopic(topic, life, buildTraceId())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("created")
}

func list() {
	pub, err := client.NewPubClient(*host, *port, time.Second*120)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer pub.Close()
	content, err := pub.GetTopicList(buildTraceId())

	if err != nil {
		fmt.Println(err)
		return
	}
	if len(content) == 0 {
		fmt.Println("no topic list")
		return
	}
	var result interface{}
	err = json.Unmarshal([]byte(content), &result)
	if err != nil {
		fmt.Println(err)
		return
	}
	pretty, err := json.MarshalIndent(result, "", "    ")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(pretty))
}

func deleteTopic(name string) {
	pub, err := client.NewPubClient(*host, *port, time.Second*120)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer pub.Close()

	if err = pub.DeleteTopic(name, buildTraceId()); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("deleted")
}

func buildTraceId() string {
	return fmt.Sprintf("traceId-cli-%d", time.Now().Unix())
}
