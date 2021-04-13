package kdbtdx

import (
	"os"
	"os/signal"
	"sync/atomic"

	logger "github.com/alecthomas/log4go"
	"github.com/robfig/cron/v3"
)

type TradeApi interface {
	LoadCfg() *Cfg
	Trade(o Order)
	Cancel(c CancelReq)
	GetUpdatedInfo() chan *Order
	RunApi()
	Stop()
}

func Run(api TradeApi) {

	c := make(chan os.Signal)
	cronTask(c)
	
	cfg := api.LoadCfg()
	Tb = newKdb(cfg.Host, cfg.Port, cfg.Auth, cfg.DbPath, cfg.Sym, cfg.MaxId)
	Tb.start()
	defer func() {
		api.Stop()
		Tb.stop()
	}()
	process(api)
	
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
}

func process(api TradeApi) {

	go api.RunApi()

	go func() {
		for ; ; {
			select {
			case o, ok := <-Tb.orderChan:
				go func() {
					if !ok {
						return
					}
					logger.Debug("new order: %#v", o)
					o.EntrustNo = atomic.AddInt32(&Tb.localID, 1)
					o.Status = -1
					Tb.oChan <- o
					Tb.orderMap.Store(o.EntrustNo, o)
					api.Trade(*o)
				}()
			case c, ok := <-Tb.cancelChan:
				go func() {
					if !ok {
						return
					}
					logger.Debug("cancel order: %#v", c)
					api.Cancel(*c)
				}()
			}
		}
	}()

	respChan := api.GetUpdatedInfo()
	go func() {
		for ; ; {
			select {
			case resp, ok := <-respChan:
				if !ok {
					return
				}
				logger.Debug("updated order: %#v", resp)
				Tb.updateOrder(resp)
			}
		}
	}()
}

func cronTask(cos chan os.Signal) {

	go func() {
		c := cron.New(cron.WithSeconds())
		// s m h
		_, _ = c.AddFunc("00 30 16 * * *", func() {
			logger.Info("close trade system")
			cos <- os.Interrupt
			//defer Tb.stop()
			//panic("stop trade")
		})

		_, _ = c.AddFunc("00 30 22 * * *", func() {
			logger.Info("close trade system")
			cos <- os.Interrupt
			//defer Tb.stop()
			//panic("stop trade")
		})
		logger.Info("start cron tasks")
		c.Start()
	}()
}
