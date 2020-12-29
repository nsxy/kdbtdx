package kdbtdx

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	logger "github.com/alecthomas/log4go"
	kdb "github.com/nsxy/kdbgo"
	"github.com/syndtr/goleveldb/leveldb"
)

type tradeKdb struct {
	*kdb.KDBConn
	*leveldb.DB
	auth, path string
	sym        []string
	localID    int32
	symMap     map[string]bool
	orderMap   sync.Map
	cancelChan chan *CancelReq
	orderChan  chan *Order
	oChan      chan *Order
}

func newKdb(h string, p int, auth, path string, sym []string, maxId int32) *tradeKdb {

	return &tradeKdb{
		KDBConn: &kdb.KDBConn{
			Host: h,
			Port: strconv.Itoa(p),
		},
		auth:       auth,
		path:       path,
		sym:        sym,
		localID:    maxId,
		symMap:     make(map[string]bool, len(sym)),
		orderMap:   sync.Map{},
		cancelChan: make(chan *CancelReq, 10000),
		orderChan:  make(chan *Order, 10000),
		oChan:      make(chan *Order, 10000),
	}
}

func (tb *tradeKdb) connect() {

	p, _ := strconv.Atoi(tb.Port)
	var err error
	tb.KDBConn, err = kdb.DialKDBTimeout(tb.Host, p, tb.auth, time.Second*10)
	if err != nil {
		logger.Crashf("connect error: %v", err)
	}
	logger.Debug("connected with host: %v, port: %v", tb.Host, p)
}

func (tb *tradeKdb) subscribe(tab string, sym []string) {

	logger.Debug("Subscribing Kdb, table: %v, sym: %v", tab, sym)
	var symK *kdb.K
	if symNum := len(sym); symNum == 0 {
		symK = kdb.Symbol("")
	} else {
		symK = kdb.SymbolV(sym)
	}

	err := tb.AsyncCall(".u.sub", kdb.Symbol(tab), symK)
	if err != nil {
		_ = logger.Error("Failed to subscribe table: %v, sym: %v", tab, sym)
	}
}

func (tb *tradeKdb) readFromKdb() {

	go func() {

		for {
			res, _, err := tb.ReadMessage()
			if err != nil {
				_ = logger.Error("read KDB message error: %v", err)
				return
			}
			switch res.Data.(type) {
			case string:
				if msg := res.Data.(string); msg == "\"heartbeat\"" {
					logger.Debug("KDB HeartBeat")
					continue
				}
			case kdb.Table:
				logger.Info("ReadMessage Table Data: %v", res.Data.(kdb.Table))
			//
			case []*kdb.K:
				dataArr := res.Data.([]*kdb.K)

				funcName := dataArr[0].Data.(string)

				if funcName != "upd" {
					_ = logger.Error("function name is not upd, func_name: %v", funcName)
					continue
				}

				tableName := dataArr[1].Data.(string)

				if tableName == HeartBeat {
					logger.Debug("KDB HeartBeat")
					continue
				}

				var tableK kdb.Table
				switch dataArr[2].Data.(type) {
				case kdb.Table:
					tableK = dataArr[2].Data.(kdb.Table)
				case kdb.Dict:
					dic := dataArr[2].Data.(kdb.Dict)
					_ = logger.Error("received a dic, dic: %v, should be a table", dic)
					continue
				}
				length := tableK.Data[0].Len()
				switch tableName {
				case EntrustTab:
					for i := 0; i < length; i++ {
						go func(i int) {
							dat := tableK.Index(i)
							row := new(entrust)
							err := kdb.UnmarshalDict(dat, row)
							if err != nil {
								_ = logger.Error("Failed to unmarshal kdb index: %v, err: %v", dat, err)
								return
							}
							if _, ok := tb.symMap[row.Accountname]; !ok {
								return
							}
							o := &Order{
								Request: Request{
									basic: basic{
										Sym: row.Accountname,
										Qid: row.Qid,
									},
									Time:      row.Time,
									Trader:    row.Sym,
									Stockcode: row.Stockcode,
									Askprice:  row.Askprice,
									Orderqty:  row.Askvol,
									Ordertype: 1,
								},
							}
							if row.Askvol < 0 {
								o.Orderqty = - o.Orderqty
								o.Side = 1
								// sell / ss market order
								if o.Askprice >= 5000 {
									o.Ordertype = 0
								}
								if strings.HasPrefix(row.Qid, "SHORT") {
									o.Side = 3
								}
							} else {
								// buy / btc market order
								if o.Askprice <= 0.1 {
									o.Ordertype = 0
								}
								if strings.HasPrefix(row.Qid, "BTC") {
									o.Side = 2
								}
							}
							tb.orderChan <- o
						}(i)
					}
				case RequestTab:
					for i := 0; i < length; i++ {
						go func(i int) {
							dat := tableK.Index(i)
							row := new(Order)
							err := kdb.UnmarshalDict(dat, row)
							if err != nil {
								_ = logger.Error("Failed to unmarshal kdb index: %v, err: %v", dat, err)
								return
							}
							tb.orderChan <- row
						}(i)
					}
				case CancelTab:
					for i := 0; i < length; i++ {
						go func(i int) {
							dat := tableK.Index(i)
							row := new(CancelReq)
							err := kdb.UnmarshalDict(dat, row)
							if err != nil {
								_ = logger.Error("Failed to unmarshal kdb index: %v, err: %v", dat, err)
								return
							}
							tb.cancelChan <- row
						}(i)
					}
				}
			}
		}
	}()
}

func (tb *tradeKdb) send2Tab(funcName, tab string, data *kdb.K) {

	if err := tb.AsyncCall(funcName, kdb.Symbol(tab), data); err != nil {
		_ = logger.Error("send data: %v to kdb table: %v with Func: %v, error: %v", data, tab, funcName)
	}
}

func (tb *tradeKdb) slice2Map() {

	for _, s := range tb.sym {
		tb.symMap[s] = true
	}
}

func (tb *tradeKdb) openDB() {

	var err error
	leveldbPath := fmt.Sprintf("%s/%s/orderInfo", tb.path, time.Now().Format("20060102"))
	tb.DB, err = leveldb.OpenFile(leveldbPath, nil)
	if err != nil {
		logger.Crashf("open db err: %v", err)
	}

	iter := tb.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		idx, err := strconv.Atoi(string(key))
		if err != nil {
			_ = logger.Error("Convert order key from local db, err: %v", err)
			continue
		}
		value := iter.Value()
		order := new(Order)
		if err = json.Unmarshal(value, order); err != nil {
			_ = logger.Error("Unmarshal order error from local db, err: %v", err)
			continue
		}
		if int32(idx) > tb.localID {
			tb.localID = order.EntrustNo
		}
		tb.orderMap.Store(int32(idx), order)
		// tb.orderMap[int32(idx)] = order
	}
	iter.Release()

	if err = iter.Error(); err != nil {
		_ = logger.Error("Load kdb order err: %v", err)
	}
}

func (tb *tradeKdb) save2Db(o *Order) {

	// save obj to local leveldb
	ordJson, err := json.Marshal(o)
	if err != nil {
		_ = logger.Error("Marshal order info error: %v", err)
		return
	}

	if err = tb.Put([]byte(strconv.Itoa(int(o.EntrustNo))), ordJson, nil); err != nil {
		_ = logger.Error("Save order info to db error: %v", err)
	}
}

func (tb *tradeKdb) save() {

	go func() {
		for o := range tb.oChan {
			tb.save2Db(o)
		}
	}()
}

func (tb *tradeKdb) updateTab(o *Order) {

	go tb.send2Tab(FuncUpdate, ResponseTabV3, ord2resp(o))
	go tb.send2Tab(FuncUpdate, ResponseTab, ord2entrust(o))
}

func (tb *tradeKdb) updateOrder(o *Order) {

	oi, ok := tb.orderMap.Load(o.EntrustNo)
	if !ok {
		_ = logger.Error("found no order which matched with %#v", o)
		return
	}
	if oo := oi.(*Order); oo.Status >= o.Status && oo.CumQty >= o.CumQty {
		logger.Debug("don't update, order info: %#v", oo)
		return
	}
	logger.Debug("send to kdb, order info: %#v", o)
	go tb.updateTab(o)
	tb.oChan <- o
	tb.orderMap.Store(o.EntrustNo, o)
}

func (tb *tradeKdb) querySql() ([]*queryResult, error) {

	acct := strings.Join(tb.sym, "`")
	query1 := fmt.Sprintf("select entrustno,status,cumqty:abs(bidvol) from %s where accountname in `%s, status < 4", ResponseTab, acct)
	query2 := fmt.Sprintf("select entrustno,status,cumqty from %s where sym in `%s, status < 4", ResponseTabV3, acct)
	query := fmt.Sprintf("distinct select from (%s) uj (%s) where status = (min;status) fby entrustno, cumqty = (min;cumqty) fby entrustno",
		query1, query2)

	res, err := tb.Call(query)
	if err != nil {
		return nil, err
	}

	var v []*queryResult
	switch res.Data.(type) {
	case kdb.Table:
		t := res.Data.(kdb.Table)
		for i := 0; i < t.Data[0].Len(); i++ {
			dat := t.Index(i)
			row := new(queryResult)
			err := kdb.UnmarshalDict(dat, row)
			if err != nil {
				_ = logger.Error("Failed to unmarshal kdb index: %v, err: %v", dat, err)
				continue
			}
			v = append(v, row)
		}
	}
	return v, nil
}

func (tb *tradeKdb) updateAfterReboot() {

	v, err := tb.querySql()
	if err != nil {
		_ = logger.Error("query err: %v", err)
		return
	}
	for ix := range v {
		vv := v[ix]
		oi, ok := tb.orderMap.Load(vv.Entrustno)
		if !ok {
			_ = logger.Error("can't find order with entrustNo: %v in memory", vv.Entrustno)
			continue
		}
		if o := oi.(*Order); vv.Status < o.Status || (vv.Status == o.Status && vv.Cumqty < o.CumQty) {
			tb.updateTab(o)
		}
	}
}

func (tb *tradeKdb) start() {

	tb.connect()
	tb.slice2Map()
	tb.openDB()
	tb.save()
	tb.updateAfterReboot()

	tb.subscribe(RequestTab, tb.sym)
	tb.subscribe(CancelTab, tb.sym)

	tb.subscribe(EntrustTab, []string{})
	tb.subscribe(HeartBeat, []string{})
	tb.readFromKdb()
}

func (tb *tradeKdb) stop() {

	logger.Debug("trade engine is stopping")
	_ = tb.KDBConn.Close()
	_ = tb.DB.Close()
	close(tb.cancelChan)
	close(tb.orderChan)
}
