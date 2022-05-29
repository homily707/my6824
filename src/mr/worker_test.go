package mr

import (
	"fmt"
	"log"
	"plugin"
	"testing"
)

func Test_handleMapOrder(t *testing.T) {
	//type args struct {
	//	order   Job
	//	mapf    func(string, string) []KeyValue
	//	reducef func(string, []string) string
	//}

	//tests := []struct {
	//	name  string
	//	args  args
	//	want  WorkerRequest
	//	want1 bool
	//}{
	//	args{},
	//}
	//for _, tt := range tests {
	//	t.Run(tt.name, func(t *testing.T) {
	//		got, got1 := handleMapOrder(tt.args.order, tt.args.mapf, tt.args.reducef)
	//		if !reflect.DeepEqual(got, tt.want) {
	//			t.Errorf("handleMapOrder() got = %v, want %v", got, tt.want)
	//		}
	//		if got1 != tt.want1 {
	//			t.Errorf("handleMapOrder() got1 = %v, want %v", got1, tt.want1)
	//		}
	//	})
	//}

	mapf, reducef := loadPlugin("wc.so")
	request, finished := HandleMapOrder(Job{0, 10, "pg-grimm.txt"}, mapf, reducef)
	fmt.Print(request.WorkerStatus, finished)
}

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
