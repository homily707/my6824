package main

import (
	"6.824/mr"
	"log"
	"os"
	"plugin"
)

func main() {
	job := mr.MapJob{Index: 0, NReduce: 10, FileName: "pg-grimm.txt"}
	mapf, _ := loadPlugin(os.Args[1])
	mr.Mapper(job, mapf)
}

func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
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
