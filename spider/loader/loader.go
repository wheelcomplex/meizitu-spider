package loader

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

type Loader struct {
	folder string
}

func NewLoader() *Loader {
	return &Loader{}
}
func (l *Loader) SetFolder(folder string) {
	l.folder = folder
}

func (l *Loader) Down(url string) {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s\n", err)
		return
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s\n", err)
		return
	}
	str := strings.Split(url, "/")
	length := len(str)
	filename := str[length-4:]
	file := strings.Join(filename, "")
	fh, err := os.Create(l.folder + file)
	defer fh.Close()
	if err != nil {
		log.Println("Down")
		fmt.Fprintf(os.Stdout, "%s\n", err)
		return
	}
	fh.Write(data)
}
