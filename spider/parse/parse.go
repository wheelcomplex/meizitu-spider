package parse

import (
	"fmt"
	"log"
	"os"
	"regexp"
)

func ParseImg(html string) ([]string, error) {
	re, err := regexp.Compile(`(src=")(http://.*\.com.*\.jpg)`)
	if err != nil {
		log.Println("ParseImg")
		fmt.Fprintf(os.Stdout, "%s\n", err)
		return nil, err
	}

	var urls []string
	match := re.FindAllStringSubmatch(html, -1)
	for _, url := range match {
		urls = append(urls, url[2])
	}
	return urls, nil
}

func ParseHtml(html string) ([]string, error) {
	re, err := regexp.Compile(`(href=")(http://www\.meizi.*\.com.*\.html)`)
	if err != nil {
		log.Println("ParseHtml")
		fmt.Fprintf(os.Stdout, "%s\n", err)
		return nil, err
	}
	var urls []string
	match := re.FindAllStringSubmatch(html, -1)
	for _, url := range match {
		urls = append(urls, url[2])
	}
	return urls, nil
}
