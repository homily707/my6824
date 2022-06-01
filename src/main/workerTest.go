package main

import (
	"6.824/mr"
	"fmt"
	"log"
	"plugin"
)

func main() {

	mapf, reducef := loadPlugin("wc.so")
	//request, finished := mr.HandleMapOrder(mr.Job{0, 8, 10, "pg-grimm.txt"}, mapf, reducef)
	request, finished := mr.HandleReduceOrder(mr.Job{0, 1, 10, ""}, mapf, reducef)
	fmt.Print(request.MessageType, finished)
}

func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		println(err.Error())
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
