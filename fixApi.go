package kdbTdxV3

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	logger "github.com/alecthomas/log4go"
	"github.com/quickfixgo/field"
	"github.com/quickfixgo/quickfix"
)

// TradeClient implements the quickFix Application interface
type fixApi struct {
	*quickfix.Initiator
	msgChan chan *quickfix.Message
}

// OnCreate implemented as part of Application interface
func (f fixApi) OnCreate(sessionID quickfix.SessionID) {
	return
}

// OnLogon implemented as part of Application interface
func (f fixApi) OnLogon(sessionID quickfix.SessionID) {
	logger.Info("----------------Login success-----------------")
	return
}

// OnLogout implemented as part of Application interface
func (f fixApi) OnLogout(sessionID quickfix.SessionID) {
	logger.Info("----------------logout success----------------------")
	return
}

// FromAdmin implemented as part of Application interface
func (f fixApi) FromAdmin(msg *quickfix.Message, sessionID quickfix.SessionID) (reject quickfix.MessageRejectError) {
	return
}

// ToAdmin implemented as part of Application interface
func (f fixApi) ToAdmin(msg *quickfix.Message, sessionID quickfix.SessionID) {
	return
}

// ToApp implemented as part of Application interface
func (f fixApi) ToApp(msg *quickfix.Message, sessionID quickfix.SessionID) (err error) {
	return
}

// FromApp implemented as part of Application interface. This is the callback for all Application level messages from the counter party.
func (f fixApi) FromApp(msg *quickfix.Message, sessionID quickfix.SessionID) (reject quickfix.MessageRejectError) {
	f.msgChan <- msg
	return
}

func newApi(fixSessionFilePath string) *fixApi {

	cfg, err := os.Open(fixSessionFilePath)
	defer func() {
		_ = cfg.Close()
	}()
	if err != nil {
		_ = logger.Error("Error opening %v, %v", fixSessionFilePath, err)
		return nil
	}

	appSettings, err := quickfix.ParseSettings(cfg)
	if err != nil {
		_ = logger.Error("Error reading cfg", err)
		return nil
	}

	fixApi := new(fixApi)
	fixApi.msgChan = make(chan *quickfix.Message, 100000)

	fileLogFactory, err := quickfix.NewFileLogFactory(appSettings)
	if err != nil {
		_ = logger.Error("Error creating file log factory", err)
		return nil
	}

	fixApi.Initiator, err = quickfix.NewInitiator(fixApi, quickfix.NewFileStoreFactory(appSettings), appSettings, fileLogFactory)
	if err != nil {
		_ = logger.Error("Unable to create Initiator", err)
		return nil
	}
	return fixApi
}

type ConditionParam struct {
	Func          string       `json:"func"`
	Field         string       `json:"field"`
	ComparedField string       `json:"comparedField"`
	Tag           quickfix.Tag `json:"tag"`
	Value         string       `json:"value"`
}

type TradeCfg struct {
	Host               string   `json:"kdb_host"`
	Port               int      `json:"kdb_port"`
	Auth               string   `json:"auth"`
	SubAccounts        []string `json:"accounts"`
	FixSessionFilePath string   `json:"fixFilePath"`
	DataCfg            string   `json:"dataCfgPath"`
	Broker             string   `json:"broker"`
	MaxNo              int32    `json:"maxNo"`
}

type DataInfo struct {
	AlgorithmTagDict map[string]quickfix.Tag            `json:"TagDict"`
	FixAcctMap       map[string]string                  `json:"fixAcctMap"`
	FixLogPath       string                             `json:"fixLogPath"`
	CondParams       []ConditionParam                   `json:"condParams"`
	RequiredParams   map[quickfix.Tag]string            `json:"requiredParams"`
	TagMap           map[quickfix.Tag]map[string]string `json:"tagMap"`
	TargetCompID     string                             `json:"targetCompID"`
	SenderCompID     string                             `json:"senderCompID"`
	KdbLeveldbPath   string                             `json:"dbPath"`
}

// an example of API
type FixApi struct {
	respChan chan *Order
	*DataInfo
	*fixApi
	TradeCfg
}

func NewFixApi() *FixApi {

	return &FixApi{
		respChan: make(chan *Order, 10000),
	}
}

func (f *FixApi) LoadConfig(cfgFilePath string) {

	data, err := ioutil.ReadFile(cfgFilePath)
	if err != nil {
		logger.Crash("Read cfgFilePath err", err)
	}
	cfg := new(TradeCfg)
	if err = json.Unmarshal(data, cfg); err != nil {
		logger.Crash("Unmarshal error", err)
	}
	if data, err = ioutil.ReadFile(cfg.DataCfg); err != nil {
		logger.Crash("Read Data cfgFilePath err", err)
	}

	f.DataInfo = new(DataInfo)
	if err = json.Unmarshal(data, f.DataInfo); err != nil {
		logger.Crash("Unmarshal DataInfo error", err)
	}

	f.FixLogPath = fmt.Sprintf("%s/FIX.4.2-%s-%s.messages.current.log", f.FixLogPath, f.SenderCompID, f.TargetCompID)

	f.fixApi = newApi(cfg.FixSessionFilePath)
}

func (f *FixApi) LoadCfg() *Cfg {

	return &Cfg{
		Host:   f.Host,
		Port:   f.Port,
		Auth:   f.Auth,
		DbPath: f.KdbLeveldbPath,
		Sym:    f.SubAccounts,
		MaxId:  f.MaxNo,
	}
}

type header interface {
	Set(f quickfix.FieldWriter) *quickfix.FieldMap
}

func (f *FixApi) queryHeader(h header) {
	h.Set(field.NewSenderCompID(f.SenderCompID))
	h.Set(field.NewTargetCompID(f.TargetCompID))
	h.Set(field.NewSendingTime(time.Now()))
}

func (f *FixApi) Trade(o Order) {

	time.Sleep(1 * time.Second)
	f.update(o)
}

func (f *FixApi) Cancel(c CancelReq) {

}

func (f *FixApi) GetUpdatedInfo() chan *Order {

	return f.respChan
}

func (f *FixApi) RunApi() {

}

func (f *FixApi) update(o Order) {

	time.Sleep(2 * time.Second)
	o.Status = 1
	f.respChan <- &o
}
