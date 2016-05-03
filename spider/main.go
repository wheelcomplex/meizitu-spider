// spider project main.go
package main

import (
	"fmt"
	"spider/loader"
	"spider/net"
	"spider/parse"
	"spider/redis"
)

const (
	startUrl = "http://www.meizitu.com"
)

var m map[string]bool

func init() {
	m = make(map[string]bool)
}
func CheckTask(url string) bool {
	_, exsits := m[url]
	return exsits
}

func addTask(html string) {
	links, err := parse.ParseHtml(html)
	if err != nil {
		return
	}
	for _, link := range links {
		if !CheckTask(link) {
			redis.SaveTask(link)
		}
	}
}

func downLoad(l *loader.Loader, html string) {
	imgs, err := parse.ParseImg(html)
	if err != nil {
		return
	}
	for _, img := range imgs {
		fmt.Println(img)
		l.Down(img)
	}
}
func main() {
	l := loader.NewLoader()
	l.SetFolder("D:/pic2/")

	//初始化任务队列
	redis.SaveTask(startUrl)

	for {
		url, err := redis.GetTask()
		if err != nil {
			break
		}
		if CheckTask(url) {
			continue
		}
		html := net.GetHtml(url)
		go downLoad(l, html)
		go addTask(html)
		m[url] = true
	}
}
