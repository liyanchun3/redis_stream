package main

import (
	"strconv"
	"time"

	"redis/stream"
)

func Producer(id int){
	stream_id :=1
	for{
		stream.NewRedis().Xadd("mystream","*",map[string]interface{}{"id":"producer"+strconv.Itoa(id)+":"+strconv.Itoa(stream_id)})
		stream_id++
		time.Sleep(time.Second)
	}
}

func main() {
	for i:=1;i<=3;i++{
		go Producer(i)
	}
	for{
		select{}
	}
}