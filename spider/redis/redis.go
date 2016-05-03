package redis

import (
	"fmt"

	"github.com/astaxie/goredis"
)

var client goredis.Client

func init() {
	client.Addr = "127.0.0.1:6379"
	client.Db = 13
}

func GetTask() (string, error) {
	data, err := client.Lpop("spider")
	if err != nil {
		fmt.Println("获取任务失败")
		fmt.Print(err)
		return "", err
	}
	if data == nil {
		return "", err
	}
	return string(data), nil
}

func SaveTask(url string) error {
	err := client.Lpush("spider", []byte(url))
	return err
}
