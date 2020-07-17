package main

import (
	"flag"
	"fmt"
	"time"

	"redis/stream"
)

func Xack(data []stream.Stream){
	time.Sleep(time.Millisecond*100)
	for _,v:=range data{
		key:= v.Name
		for _,msg:=range v.Msgs{
			stream.NewRedis().Xack(key,"my_group",msg.Id)
		}
	}
}

func main() {
	var consumerName = flag.String("consumer", "user1", "消费者名称")
	flag.Parse()
	fmt.Println(*consumerName)
	stream.NewRedis().XgroupCreate("mystream","my_group","0",true)
	for{
		data,_:=stream.NewRedis().XreadGroup("my_group",*consumerName,[]interface{}{"mystream"},[]interface{}{">"},nil,100000)
		fmt.Printf("%s %v\n",time.Now().Format("2006-01-02 15:04:05"),data)
		go Xack(data)
	}
}