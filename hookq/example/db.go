package main

import "gorm.io/gorm"

//OrderAction order action
type OrderAction struct {
	//ID message id
	ID int64 `msgpack:"id"`
	//State
	State int32 `msgpack:"state"`
}

//OrderState order state
type OrderState struct {
	//ID message id
	ID int64 `gorm:"column:id"`
	//State
	State int32 `gorm:"column:state"`
}

//OrderStateProc : db store
type OrderStateProc struct {
	tbl string
	o   *OrderState
}

//NewOrderStateProc : new order state proc
func NewOrderStateProc(o *OrderState, tlb string) *OrderStateProc {
	return &OrderStateProc{
		tbl: tlb,
		o:   o,
	}
}

func (o *OrderStateProc) Save() func(*gorm.DB) error {
	return func(db *gorm.DB) error {
		return db.Table(o.tbl).Create(o.o).Error
	}
}

func (o *OrderStateProc) Msg() interface{} {
	return &OrderAction{
		ID:    o.o.ID,
		State: o.o.State,
	}
}
