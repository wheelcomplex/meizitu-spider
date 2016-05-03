package net

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

func GetHtml(url string) string {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s\n", err)
		os.Exit(1)
	}
	return string(body)
}
