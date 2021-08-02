package kdbtdx

import (
	"time"
)

const (
	EntrustTab    = "requestv2"
	RequestTab    = "requestv3"
	CancelTab     = "cancelTab"
	HeartBeat     = "HeartBeat"
	ResponseTab   = "response"
	ResponseTabV3 = "responsev3"
	FuncUpdate    = "wsupdv2"
)

var (
	entrustCols = []string{"sym", "qid", "accountname", "time", "entrustno", "stockcode", "askprice", "askvol",
		"bidprice", "bidvol", "withdraw", "status"}

	respCols = []string{"sym", "qid", "resptime", "orderID", "securityID", "entrustno", "cumqty", "avgpx", "status", "note"}

	Tb  = &tradeKdb{}
)

type basic struct {
	Sym, Qid string
}

type entrust struct {
	basic
	Accountname              string
	Time                     time.Time
	Entrustno                int32
	Stockcode                string
	Askprice                 float64
	Askvol                   int32
	Bidprice                 float64
	Bidvol, Withdraw, Status int32
}

type Request struct {
	basic
	Time                                                time.Time
	ParentId, Trader, Fund, Strategy, Broker, Stockcode string
	Stocktype, Side                                     int32
	Askprice                                            float64
	Ordertype, Orderqty                                 int32
	Algorithm, Params                                   string
}

type CancelReq struct {
	basic
	Entrustno int32
}

type Order struct {
	Request
	OrderId           string
	SecurityId        string
	EntrustNo, CumQty int32
	AvgPx             float64
	Withdraw, Status  int32
	Note              string
}


type FullKdbCfg struct {
	Host   string   `json:"host"`
	Port   int      `json:"port"`
	Auth   string   `json:"auth"`
}

type Cfg struct {
	DbPath string   `json:"dbPath"`
	Host   string   `json:"host"`
	Port   int      `json:"port"`
	Auth   string   `json:"auth"`
	Sym    []string `json:"sym"`
	MaxId  int32    `json:"maxId"`
	FullKdbCfg *FullKdbCfg `json:"full_kdb_cfg"`
}

type Hold struct {
	Account    string `json:"account"`
	Ticker     string `json:"ticker"`
	Count      int    `json:"count"`
	FrozenQty  int    `json:"frozenQty"`
	InitialQty int    `json:"initialQty"`
}

type queryResult struct {
	Entrustno, Status, Cumqty int32
}
