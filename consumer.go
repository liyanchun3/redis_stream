package main

import (
	"flag"
	"fmt"
	"time"

	"redis/stream"
)

func main() {
	var consumerName = flag.String("consumer", "user1", "消费者名称")
	flag.Parse()
	fmt.Println(*consumerName)
	stream.NewRedis().XgroupCreate("mystream","my_group","0")
	for{
		data,_:=stream.NewRedis().XreadGroup("my_group",*consumerName,[]interface{}{"mystream"},[]interface{}{">"},nil,100000)
		fmt.Printf("%s %v\n",time.Now().Format("2006-01-02 15:04:05"),data)
	}
}