package kdbTdxV3

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	logger "github.com/alecthomas/log4go"
	"github.com/google/uuid"
	"github.com/quickfixgo/enum"
	"github.com/quickfixgo/field"
	fix42nos "github.com/quickfixgo/fix42/newordersingle"
	fix42cxl "github.com/quickfixgo/fix42/ordercancelrequest"
	"github.com/quickfixgo/quickfix"
	"github.com/quickfixgo/tag"
	"github.com/shopspring/decimal"
)

// TradeClient implements the quickFix Application interface
type fixApi struct {
	*quickfix.Initiator
	msgChan chan string
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
	f.msgChan <- msg.String()
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
	fixApi.msgChan = make(chan string, 100000)

	fileLogFactory, err := quickfix.NewFileLogFactory(appSettings)
	if err != nil {
		_ = logger.Error("Error creating file log factory", err)
		return nil
	}
	// fixApi.Initiator, err = quickfix.NewInitiator(fixApi, quickfix.NewMongoStoreFactory(appSettings), appSettings, fileLogFactory)
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
	// cfg := new(TradeCfg)
	if err = json.Unmarshal(data, &f.TradeCfg); err != nil {
		logger.Crash("Unmarshal error", err)
	}
	if data, err = ioutil.ReadFile(f.DataCfg); err != nil {
		logger.Crash("Read Data cfgFilePath err", err)
	}

	f.DataInfo = new(DataInfo)
	if err = json.Unmarshal(data, f.DataInfo); err != nil {
		logger.Crash("Unmarshal DataInfo error", err)
	}

	f.FixLogPath = fmt.Sprintf("%s/FIX.4.2-%s-%s.messages.current.log", f.FixLogPath, f.SenderCompID, f.TargetCompID)
	f.logFile()
	f.fixApi = newApi(f.FixSessionFilePath)

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

	account := o.Sym
	code := o.Stockcode
	var side enum.Side
	switch o.Side {
	case 0, 2:
		side = enum.Side_BUY
	case 1:
		side = enum.Side_SELL
	case 3:
		side = enum.Side_SELL_SHORT
	default:
		o.Note = "wrong side"
		f.rejectOrder(o)
		return
	}
	var ordType enum.OrdType
	switch o.Ordertype {
	case 0:
		ordType = enum.OrdType_MARKET
	case 1:
		ordType = enum.OrdType_LIMIT
	default:
		o.Note = "wrong order type"
		f.rejectOrder(o)
		return
	}
	var sfx string
	switch o.Stocktype {
	case 1:
		sfx = queryConnSfx(code)
	case 2:
		sfx = queryQFiiSfx(code)
	default:
		o.Note = "wrong stock type"
		f.rejectOrder(o)
		return
	}
	if sfx == "" {
		o.Note = "unknown stock type"
		f.rejectOrder(o)
		return
	}

	o.SecurityId = fmt.Sprintf("%s.%s", code, sfx)
	ordID := pack(account, o.EntrustNo)
	fixOrder := fix42nos.New(field.NewClOrdID(ordID), field.NewHandlInst("1"),
		field.NewSymbol(code), field.NewSide(side), field.NewTransactTime(time.Now()),
		field.NewOrdType(ordType),
	)
	fixOrder.Set(field.NewIDSource("5"))
	fixOrder.Set(field.NewCurrency("CNY"))
	fixOrder.Set(field.NewSecurityID(o.SecurityId))
	fixOrder.SetString(100, sfx)
	fixOrder.Set(field.NewOrderQty(decimal.NewFromFloat32(float32(o.Orderqty)), 0))
	fixOrder.Set(field.NewTimeInForce(enum.TimeInForce_DAY))

	switch ordType {
	case enum.OrdType_LIMIT, enum.OrdType_STOP_LIMIT:
		fixOrder.Set(field.NewPrice(decimal.NewFromFloat(o.Askprice), 2))
	}

	if tagAcct := f.FixAcctMap[account]; tagAcct != "" {
		fixOrder.SetAccount(tagAcct)
	}

	if o.Algorithm == "" {
		o.Algorithm = "DMA"
	}
	if tagAlgorithm, ok := f.AlgorithmTagDict[o.Algorithm]; ok {
		fixOrder.SetString(tagAlgorithm, getValueString(f.TagMap[tagAlgorithm], o.Algorithm, o.Algorithm))
	}

	var params map[string]string
	if o.Params != "" {
		err := json.Unmarshal([]byte(o.Params), &params)
		if err != nil {
			o.Note = "params unmarshal err"
			f.rejectOrder(o)
			return
		}
	}
	// params
	for k, v := range params {
		if v != "" {
			tagK := f.AlgorithmTagDict[k]
			fixOrder.SetString(tagK, getValueString(f.TagMap[tagK], v, v))
		}
	}

	for k, v := range f.RequiredParams {
		if v != "" {
			fixOrder.SetString(k, v)
		}
	}

	orderMap := StructToMap(o.Request)

	for _, v := range f.CondParams {
		switch v.Func {
		case "=":
			if orderMap[v.Field] == v.ComparedField {
				fixOrder.SetString(v.Tag, v.Value)
			}
		case "!=":
			if orderMap[v.Field] != v.ComparedField {
				fixOrder.SetString(v.Tag, v.Value)
			}
		case "startsWith":
			if strings.HasPrefix(orderMap[v.Field], v.ComparedField) {
				fixOrder.SetString(v.Tag, v.Value)
			}
		case "set":
			fixOrder.SetString(v.Tag, orderMap[v.Field])
		default:
			_ = logger.Warn("no define func", v.Func)
		}
	}

	msg := fixOrder.ToMessage()
	f.queryHeader(&msg.Header)

	if err := quickfix.Send(fixOrder); err != nil {
		_ = logger.Error("Fix Msg Send error", err)
		o.Note = "Fix Msg Send error"
		f.rejectOrder(o)
		return
	}
	o.Status = 0
	// f.respChan <- &o
	Store(&o)
}

func (f *FixApi) Cancel(c CancelReq) {

	entrust, ok := GetOrder(c.Entrustno)

	if ok {
		logger.Info("get entrustNo: %d", c.Entrustno)

		account := entrust.Sym
		origIDPack := pack(account, c.Entrustno)
		packID := fmt.Sprintf("C-%s-%s", getUid(2), origIDPack)
		var side enum.Side
		switch entrust.Side {
		case 0, 2:
			side = enum.Side_BUY
		case 1:
			side = enum.Side_SELL
		case 3:
			side = enum.Side_SELL_SHORT
		}
		order := fix42cxl.New(field.NewOrigClOrdID(origIDPack), field.NewClOrdID(packID),
			field.NewSymbol(entrust.Stockcode), field.NewSide(side), field.NewTransactTime(time.Now()))

		order.Set(field.NewSecurityID(entrust.SecurityId))
		order.Set(field.NewOrderQty(decimal.NewFromFloat(float64(entrust.Orderqty)), 0))
		order.SetString(100, getSfx(entrust.SecurityId))
		msg := order.ToMessage()
		f.queryHeader(&msg.Header)
		if err := quickfix.Send(order); err != nil {
			_ = logger.Error("Fix Cancel Msg Send error", err)
		}
		return
	}
	_ = logger.Warn("no entrustNo: %d", c.Entrustno)
}

func (f *FixApi) GetUpdatedInfo() chan *Order {

	return f.respChan
}

func (f *FixApi) RunApi() {

	go f.update()
	f.updateAfterReboot()
	if err := f.Start(); err != nil {
		logger.Crash("fix initiator start err: %v", err)
	}
}

func (f *FixApi) Stop() {

	f.Initiator.Stop()
	close(f.respChan)
}

func (f *FixApi) update() {

	logger.Info("::::::Update FixResp Data::::::")
	RespOrdReg := regexp.MustCompile("35=8")

	for msg := range f.msgChan {
		go func(resp string) {
			if RespOrdReg.MatchString(resp) {
				entrustNo := priorTag(tag.OrigClOrdID, tag.ClOrdID, resp, "")
				status := getStatus(resp)
				logger.Info(":::receive order entrustNo %v, status %v :::", entrustNo, status)
				if entrustNo == "" || status == "3" {
					return
				}
				entrustInt := unpack(entrustNo)
				o, ok := GetOrder(entrustInt)
				if ok {

					logger.Info("get local Entrust No, %d", entrustInt)
					logger.Info("info::: %#v", o)

					cumQty := getTag(tag.CumQty, resp, "")
					cumQtyInt, _ := strconv.Atoi(cumQty)

					if status == "1" && int(o.CumQty) >= cumQtyInt {
						return
					}

					if status == "4" {
						o.Withdraw = o.Orderqty - int32(cumQtyInt)
					}

					// unknown status
					o.Status = -1
					if statusInt, statusOk := StatusMapInt32[status]; statusOk {
						o.Status = statusInt
					}
					o.CumQty = int32(cumQtyInt)
					if o.OrderId == "" {
						o.OrderId = entrustNo
					}
					AvgPx := getTag(tag.AvgPx, resp, "")
					o.AvgPx, _ = strconv.ParseFloat(AvgPx, 32)
					if status == "8" {
						o.Note = getTag(tag.Text, resp, "")
					}
					f.respChan <- &o
				} else {
					_ = logger.Error("no found entrust info in entrust data: %v", entrustNo)
				}
			}
		}(msg)
	}
}

func (f *FixApi) updateAfterReboot() {

	entrusts := GetUnFinalizedOrderNo()

	if len(entrusts) < 1 {
		return
	}

	if !checkFile(f.FixLogPath) {
		return
	}

	logReader, err := os.Open(f.FixLogPath)
	defer logReader.Close()
	if err != nil {
		_ = logger.Warn("Open fix log file %v err: %v", f.FixLogPath, err)
		return
	}

	RespOrdReg := regexp.MustCompile("35=8")
	lineScanner := bufio.NewScanner(logReader)

	for lineScanner.Scan() {
		line := lineScanner.Text()
		if RespOrdReg.MatchString(line) {
			idx := priorTag(tag.OrigClOrdID, tag.ClOrdID, line, "")
			idxInt := unpack(idx)
			if _, ok := entrusts[idxInt]; ok {
				f.msgChan <- line
			}
		}
	}
}

func (f *FixApi) logFile() {

	if !checkFile(f.FixLogPath) {
		return
	}

	logReader, err := os.Open(f.FixLogPath)
	if err != nil {
		_ = logger.Warn("Open log file %v err: %v", f.FixLogPath, err)
		_ = logReader.Close()
		return
	}

	lineScanner := bufio.NewScanner(logReader)
	moveFlag := false
	toName := time.Now().Format("20060102-150405")
	for lineScanner.Scan() {
		line := lineScanner.Text()
		if dt := getTag(tag.SendingTime, line, ""); dt != "" {
			tm := strings.Split(dt, "-")[0]
			if tm < time.Now().Format("20060102") {
				moveFlag = true
			}
			toName = tm
			break
		}
	}
	_ = logReader.Close()

	if moveFlag {

		logPaths := strings.Split(f.FixLogPath, "/")
		n := len(logPaths)

		histPath := fmt.Sprintf("%s/%s", strings.Join(logPaths[:n-1], "/"), "hist")
		if err := os.MkdirAll(histPath, os.ModePerm); err != nil {
			_ = logger.Error("create hist dir err", err)
		}
		if err := os.Rename(f.FixLogPath, fmt.Sprintf("%s/%s-%s.log", histPath, f.Broker, toName)); err != nil {
			_ = logger.Error("move messages log file error", err)
		}

		recyclePath := fmt.Sprintf("%s/%s", strings.Join(logPaths[:n-1], "/"), "recycle")
		if err := os.MkdirAll(recyclePath, os.ModePerm); err != nil {
			_ = logger.Error("create recycle dir err", err)
		}
		eventLog := strings.Replace(f.FixLogPath, "messages", "event", 1)
		if err := os.Rename(eventLog, fmt.Sprintf("%s/%s-%s-%s.log", recyclePath, f.Broker, "event", toName)); err != nil {
			_ = logger.Error("move event log file error", err)
		}
	}

}

func (f *FixApi) rejectOrder(o Order) {

	o.Status = 6
	f.respChan <- &o
}

func queryConnSfx(stockCode string) string {

	switch stockCode[0] {
	case '0', '3':
		return "ZK"
	case '5', '6':
		return "SH"
	}
	return ""
}

func queryQFiiSfx(stockCode string) string {

	switch stockCode[0] {
	case '0', '3':
		return "SZ"
	case '5', '6':
		return "SS"
	}
	return ""
}

func pack(acct string, id int32) string {

	return fmt.Sprintf("%s-%s-%d", acct, nowDt, id)
}

func unpack(entrustNo string) int32 {

	if strings.Contains(entrustNo, "-") {
		strSlice := strings.Split(entrustNo, "-")
		n := len(strSlice) - 1
		idx, err := strconv.Atoi(strSlice[n])
		if err == nil {
			return int32(idx)
		}
		return -1
	}
	return -1
}

func getValueString(dataMap map[string]string, key string, def string) string {

	if val, ok := dataMap[key]; ok {
		return val
	}
	return def
}

func StructToMap(obj interface{}) map[string]string {

	objType := reflect.TypeOf(obj)
	objVal := reflect.ValueOf(obj)
	var data = make(map[string]string)
	for i := 0; i < objType.NumField(); i++ {
		if objVal.Field(i).CanInterface() {
			data[objType.Field(i).Name] = fmt.Sprintf("%v", objVal.Field(i).Interface())
		}
	}
	return data
}

func StructToJMap(obj interface{}) map[string]string {

	m := make(map[string]string)
	j, _ := json.Marshal(obj)
	_ = json.Unmarshal(j, &m)
	return m
}

func getUid(n int) string {

	qid := strings.Replace(uuid.New().String(), "-", "", -1)
	if n >= len(qid) || n < 0 {
		return qid
	}
	return qid[:n]
}

func getTag(first quickfix.Tag, line string, def string) string {
	reg := regexp.MustCompile(fmt.Sprintf(`%d=(?U)(.*)`, first))
	if reg.MatchString(line) {
		return reg.FindStringSubmatch(line)[1]
	}
	return def
}

func priorTag(first, second quickfix.Tag, line string, def string) string {
	prior := getTag(first, line, "-999")
	if prior != "-999" {
		return prior
	}
	return getTag(second, line, def)
}

func getStatus(line string) string {
	tagExecType := getTag(150, line, "")
	tagOrdStatus := getTag(39, line, "")

	if StatusMapInt32[tagExecType] > StatusMapInt32[tagOrdStatus] {
		return tagExecType
	}
	return tagOrdStatus
}

func getSfx(s string) string {

	if len(s) < 2 {
		return ""
	}
	return s[len(s)-2:]
}

func checkFile(filePath string) bool {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		if time.Now().Format("15:04") <= "09:20" {
			logger.Info("no found %v, maybe it is first start, err: %v", filePath, err)
			return false
		}
		_ = logger.Warn("no found %v, err: %v", filePath, err)
		return false
	}
	return true
}

var (
	nowDt          = time.Now().Format("20060102")
	StatusMapInt32 = map[string]int32{
		"0": 1,
		"1": 2,
		"2": 4,
		"4": 5,
		"6": 3,
		"8": 6,
	}
)
