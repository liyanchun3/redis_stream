package stream

import (
	"errors"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

var client *redis.Pool

type Redis struct {
}

func NewRedis() *Redis {
	return &Redis{}
}

type Msg struct {
	Id      string
	Content map[string]string
}

type Stream struct {
	Name string
	Msgs []Msg
}

func Values(reply interface{}) ([]interface{}, error) {
	switch reply := reply.(type) {
	case []interface{}:
		return reply, nil
	case nil:
		return nil, errors.New("nil returned")
	}
	return nil, fmt.Errorf(" unexpected type for Values, got type %T", reply)
}

func DecodeMsg(data []interface{}) (msgs []Msg) {
	for _, msg_i := range data {
		if msg, err := Values(msg_i); err == nil {
			msg_v := Msg{Content: make(map[string]string)}
			if t,ok:=msg[0].([]uint8);ok{
				msg_v.Id = string(t)
			}
			var key string
			if content, err := Values(msg[1]); err == nil {
				for k, v := range content {
					if t,ok:=v.([]uint8);ok{
						if k%2 == 0 {
							key = string(t)
						} else {
							msg_v.Content[key] = string(t)
						}
					}

				}
				msgs = append(msgs, msg_v)
			}
		}
	}

	return msgs
}

func DecodeStream(data []interface{}) (streams []Stream) {
	for _, stream_i := range data {
		if stream, err := Values(stream_i); err == nil {
			stream_v := Stream{Msgs: make([]Msg, 0)}
			if t,ok:=stream[0].([]uint8);ok{
				stream_v.Name = string(t)
			}
			stream_v.Msgs = DecodeMsg(stream[1].([]interface{}))
			streams = append(streams, stream_v)
		}
	}
	return streams
}

func Do(commandName string, args ...interface{}) (interface{}, error) {
	c := client.Get()
	defer c.Close()
	return c.Do(commandName, args...)
}

//stream
func (r *Redis) Xadd(key string, ID string, args map[string]interface{}) (string, error) {
	var params = make([]interface{}, 0)
	params = append(params, key)
	if ID == "" {
		params = append(params, "*")
	} else {
		params = append(params, ID)
	}
	for k, v := range args {
		params = append(params, k, v)
	}
	return redis.String(Do("xadd", params...))
}

func (r *Redis) Xlen(key string) (int64, error) {
	return redis.Int64(Do("xlen", key))
}

func (r *Redis) Xdel(key string, id ...interface{}) (int64, error) {
	var params = make([]interface{}, 0)
	params = append(params, key)
	params = append(params, id...)
	return redis.Int64(Do("xdel", params...))
}

func (r *Redis) Xrange(key string, start string, end string, count ...interface{}) (data []Msg, err error) {
	var params = make([]interface{}, 0)
	params = append(params, key, start, end)
	if len(count) == 1 {
		params = append(params, "count", count[0])
	}
	msgs, err := redis.Values(Do("xrange", params...))
	if err != nil {
		return nil, err
	}
	data = DecodeMsg(msgs)
	return data, nil
}

func (r *Redis) Xrevrange(key string, end string, start string, count ...interface{}) (data []Msg, err error) {
	var params = make([]interface{}, 0)
	params = append(params, key, end, start)
	if len(count) == 1 {
		params = append(params, "count", count[0])
	}
	msgs, err := redis.Values(Do("xrevrange", params...))
	if err != nil {
		return nil, err
	}
	data = DecodeMsg(msgs)
	return data, nil
}

func (r *Redis) Xread(key []interface{}, id []interface{}, count interface{}, block interface{}) (streams []Stream, err error) {
	var params = make([]interface{}, 0)
	if count != nil {
		params = append(params, "count", count)
	}

	if block != nil {
		params = append(params, "block", block)
	}
	params = append(params, "streams")
	params = append(params, key...)
	params = append(params, id...)
	values, err := redis.Values(Do("xread", params...))
	if err != nil {
		return nil, err
	}
	streams = DecodeStream(values)

	return streams, nil
}

func (r *Redis) XgroupCreate(key, groupName, id string) (string, error) {
	return redis.String(Do("xgroup", "create", key, groupName, id))
}

func (r *Redis) XgroupDestroy(key, groupName string) (int64, error) {
	return redis.Int64(Do("xgroup", "destroy", key, groupName))
}

func (r *Redis) XgroupSetid(key, groupName, id string) (string, error) {
	return redis.String(Do("xgroup", "setid", key, groupName, id))
}

func (r *Redis) XgroupDelConsumer(key, groupName, consumerName string) (int64, error) {
	return redis.Int64(Do("xgroup", "delconsumer", key, groupName, consumerName))
}

func (r *Redis) XreadGroup(groupName, consumerName string, key []interface{}, id []interface{}, count interface{}, block interface{}) (streams []Stream, err error) {
	var params = make([]interface{}, 0)
	params = append(params, "group", groupName, consumerName)
	if count != nil {
		params = append(params, "count", count)
	}

	if block != nil {
		params = append(params, "block", block)
	}

	params = append(params, "streams")
	params = append(params, key...)
	params = append(params, id...)
	values, err := redis.Values(Do("xreadgroup", params...))
	if err != nil {
		return nil, err
	}
	streams = DecodeStream(values)

	return streams, nil
}

func (r *Redis) Xack(key, groupName string, id ...interface{}) (int64, error) {
	var params = make([]interface{}, 0)
	params = append(params, key, groupName)
	params = append(params, id...)
	return redis.Int64(Do("xack", params...))
}

func (r *Redis) Xclaim(key, groupName, consumer string, minIldeTime interface{}, id ...interface{}) (msgs []Msg, err error) {
	var params = make([]interface{}, 0)
	params = append(params, key, groupName, consumer, minIldeTime)
	params = append(params, id...)
	values, err := redis.Values(Do("xclaim", params...))
	if err != nil {
		return nil, err
	}
	msgs = DecodeMsg(values)
	return msgs, nil
}

func init() {
	client = &redis.Pool{
		MaxIdle:     30,
		MaxActive:   10,
		IdleTimeout: time.Second * 10,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return nil, err
			}
			/*if _, err := c.Do("AUTH", ""); err != nil {
				c.Close()
				return nil, err
			}*/
			if _, err := c.Do("SELECT", 0); err != nil {
				c.Close()
				return nil, err
			}
			return c, nil
		},
	}
}
