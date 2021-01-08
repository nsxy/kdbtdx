package kdbTdxV3

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestKdb(t *testing.T) {

	s := newKdb("localhost", 5001, "test:pass", "./", []string{"testMS2", "testMS022"}, 10)
	//s.connect()
	//v, err := s.querySql()
	//for ix := range v {
	//	fmt.Printf("query res: %#v\n", v[ix])
	//}
	//fmt.Println("err", err)
	//	_ = s.KDBConn.Close()
	s.start()
	defer s.stop()
}


func TestRef(t *testing.T) {

	o := Request{
		Time: time.Now(),
		ParentId:  "q01",
		Trader:    "tr",
		Fund:      "ds",
		Strategy:  "dsx",
		Broker:    "cs",
		Stockcode: "601818",
		Stocktype: 0,
		Side:      3,
		Askprice:  0.25,
		Ordertype: 1,
		Orderqty:  1,
		Algorithm: "DMA",
		Params:    "",
	}

	objType := reflect.TypeOf(o)
	objVal := reflect.ValueOf(o)

	var data = make(map[string]string)
	for i := 0; i < objType.NumField(); i++ {
		if objVal.Field(i).CanInterface() {
			data[objType.Field(i).Name] = fmt.Sprintf("%v", objVal.Field(i).Interface())
		}
	}
	fmt.Println(data)

}