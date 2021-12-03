package hookq

import (
	"github.com/pinealctx/neptune/store/gormx"
	"github.com/vmihailenco/msgpack/v5"
	"gorm.io/gorm"
	"sync"
)

//Hooker -- write data with message hook
type Hooker interface {
	//Save : save function
	Save() func(*gorm.DB) error
	//Msg : generate msg
	Msg() interface{}
}

//Producer : producer
type Producer struct {
	tbl    string     //table name
	txn    int64      //transaction id, increase 1
	locker sync.Mutex //locker
}

//NewProducer : new producer
func NewProducer(db *gorm.DB, tbl string) (*Producer, error) {
	var maxID, err = getMaxMsgID(db, tbl)
	if err != nil {
		return nil, err
	}
	var p = &Producer{
		tbl: tbl,
		txn: maxID,
	}
	return p, nil
}

//WriteHook : write data with message put
func (p *Producer) WriteHook(db *gorm.DB, h Hooker) error {
	return p.WriteHookMsgP(db, h.Save(), h.Msg())
}

//WriteHookMsgP : write data with message put(msgpack protocol) hook
func (p *Producer) WriteHookMsgP(db *gorm.DB, fn func(db *gorm.DB) error, msg interface{}) error {
	//if message is empty, don't put into message queue
	if msg == nil {
		return fn(db)
	}
	var data, err = msgpack.Marshal(msg)
	if err != nil {
		return err
	}
	p.locker.Lock()
	defer p.locker.Unlock()
	var nextID = p.txn + 1
	var oLog = &Msg{
		ID:   nextID,
		P:    MsgPack,
		Data: data,
	}
	var putFn = func(txn *gorm.DB) error {
		return addMsg(txn, p.tbl, oLog)
	}
	err = gormx.Transact(db, fn, putFn)
	if err != nil {
		return err
	}
	p.txn = nextID
	return nil
}
