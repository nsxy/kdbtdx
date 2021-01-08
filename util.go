package kdbTdxV3

import (
	"time"

	kdb "github.com/sv/kdbgo"
)

func ord2resp(order *Order) *kdb.K {

	return kdb.NewTable(respCols, []*kdb.K{
		kdb.SymbolV([]string{order.Sym}),
		kdb.SymbolV([]string{order.Qid}),
		kdb.TimeZV([]time.Time{time.Now()}),
		kdb.SymbolV([]string{order.OrderId}),
		kdb.SymbolV([]string{order.SecurityId}),
		kdb.IntV([]int32{order.EntrustNo}),
		kdb.IntV([]int32{order.CumQty}),
		kdb.FloatV([]float64{order.AvgPx}),
		kdb.IntV([]int32{order.Status}),
		kdb.SymbolV([]string{order.Note}),
	})
}

func ord2entrust(order *Order) *kdb.K {

	qty := order.Orderqty
	cumQty := order.CumQty
	if order.Side%2 == 1 {
		qty = -qty
		cumQty = -cumQty
	}

	var wd int32
	if order.Status == 5 {
		wd = order.Orderqty - order.CumQty
	}

	return kdb.NewTable(entrustCols, []*kdb.K{
		kdb.SymbolV([]string{order.Trader}),
		kdb.SymbolV([]string{order.Qid}),
		kdb.SymbolV([]string{order.Sym}),
		kdb.TimeZV([]time.Time{order.Time.Add(-8 * time.Hour)}),
		kdb.IntV([]int32{order.EntrustNo}),
		kdb.SymbolV([]string{order.Stockcode}),
		kdb.FloatV([]float64{order.Askprice}),
		kdb.IntV([]int32{qty}),
		kdb.FloatV([]float64{order.AvgPx}),
		kdb.IntV([]int32{cumQty}),
		kdb.IntV([]int32{wd}),
		kdb.IntV([]int32{order.Status}),
	})
}

func GetOrder(key int32) (Order, bool) {

	vi, ok := Tb.orderMap.Load(key)
	if !ok {
		return Order{}, false
	}
	v := vi.(*Order)
	return *v, true
}

func GetUnFinalizedOrderNo() map[int32]bool {

	res := make(map[int32]bool)
	Tb.orderMap.Range(func(_, value interface{}) bool {
		if o := value.(*Order); o.Status < 4 {
			res[o.EntrustNo] = true
		}
		return true
	})
	return res
}

func Store(o *Order) {

	Tb.updateTab(o)
	Tb.oChan <- o
	Tb.orderMap.Store(o.EntrustNo, o)
}