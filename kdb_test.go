package kdbtdx

import (
	"testing"
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
