// spider project main.go
package main

import (
	"fmt"
	"sync"

	"github.com/wheelcomplex/meizitu-spider/spider/loader"
	"github.com/wheelcomplex/meizitu-spider/spider/net"
	"github.com/wheelcomplex/meizitu-spider/spider/parse"
	"github.com/wheelcomplex/meizitu-spider/spider/taskdb"
)

var startUrl = []byte("http://www.meizitu.com")

var m map[string]bool
var lk sync.Mutex

var redis *taskdb.Taskdb

var tk taskdb.UrlTask

//
func CheckTask(url []byte) bool {
	_, exsits := m[string(url)]
	return exsits
}

func addTask(html string) {
	links, err := parse.ParseHtml(html)
	if err != nil {
		return
	}
	lk.Lock()
	for _, link := range links {
		tk.Url = []byte(link)
		if !CheckTask(tk.Url) {
			err := redis.SaveTask(&tk)
			if err != nil {
				panic(err.Error())
			}

		}
	}
	lk.Unlock()
}

func downLoad(l *loader.Loader, html string, wg *sync.WaitGroup) {
	defer wg.Done()
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
	m = make(map[string]bool)
	var err error
	redis, err = taskdb.NewDefaultTaskdb()
	if err != nil {
		panic(err.Error())
	}
	defer redis.Close()
	println("default taskdb initialed.")

	l := loader.NewLoader()
	l.SetFolder("data/pics/")

	//初始化任务队列
	tk.Url = append(tk.Url[:0], startUrl...)
	redis.SaveTask(&tk)
	if err != nil {
		panic(err.Error())
	}
	err = redis.GetTask(&tk)
	if err != nil {
		fmt.Printf("GetTask: %s\n", err.Error())
		panic("")
	}
	fmt.Printf("Get first: %s\n", tk)

	var wg sync.WaitGroup

	for {
		lk.Lock()
		err = redis.PopTask(&tk)
		if err != nil {
			fmt.Printf("PopTask: %s\n", err.Error())
			lk.Unlock()
			break
		}
		if CheckTask(tk.Url) {
			lk.Unlock()
			continue
		}
		url := string(tk.Url)
		html := net.GetHtml(url)
		go downLoad(l, html, &wg)
		go addTask(html)
		m[url] = true
		lk.Unlock()
	}
	wg.Wait()
	fmt.Printf("all done!\n")
}
