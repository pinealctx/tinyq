package hookq

import (
	"github.com/pinealctx/neptune/store/gormx"
	"gorm.io/gorm"
	"sync"
	"time"
)

//MsgHandler : msg handler
type MsgHandler interface {
	Do(msg *Msg) error
}

//ErrDo : error handler
type ErrDo interface {
	HandleErr(err error)
}

//ErrHandler : error handler
type ErrHandler struct {
	ReadErrHandler   ErrDo
	SubmitErrHandler ErrDo
	MsgErrHandler    ErrDo
}

//Consumer : consumer
type Consumer struct {
	id        string   //client id
	topicTbl  string   //topic table name
	cursorTbl string   //cursor table name
	db        *gorm.DB //db

	//handler
	mh MsgHandler  //message handler
	eh *ErrHandler //error handler

	//config
	n              int           // read n in each fetch
	tick           time.Duration //fetch message interval
	catchN         int64         //after msg consumed count to submit cursor
	catchT         time.Duration //after msg consumed time to submit cursor
	submitRetryNum int           //submit retry number

	//dynamic
	consumedID  int64     //consumed id
	submittedID int64     //submitted txn
	submittedTS time.Time //submitted time

	//control:
	startOnce  sync.Once
	stopOnce   sync.Once
	stopChan   chan struct{}
	exitSignal chan struct{}
}

//NewConsumer new consumer
func NewConsumer(cnf *Cnf, mh MsgHandler, eh *ErrHandler) (*Consumer, error) {
	var db, err = NewDB(cnf)
	if err != nil {
		return nil, err
	}

	var (
		sid         int64
		submittedTS time.Time
	)

	sid, submittedTS, err = getClientCursor(db, cnf.CursorTbl, cnf.ID)
	if err != nil {
		return nil, err
	}
	var c = &Consumer{
		id:        cnf.ID,
		topicTbl:  cnf.TopicTbl,
		cursorTbl: cnf.CursorTbl,
		db:        db,
		mh:        mh,
		eh:        eh,

		n:              cnf.N,
		tick:           cnf.Tick.Duration,
		catchN:         cnf.CatchN,
		catchT:         cnf.CatchT.Duration,
		submitRetryNum: cnf.SubmitRetryNum,

		consumedID:  sid,
		submittedID: sid,
		submittedTS: submittedTS,
		stopChan:    make(chan struct{}),
		exitSignal:  make(chan struct{}),
	}
	return c, nil
}

//Start : start consumer in go routine
func (c *Consumer) Start() {
	c.startOnce.Do(c.start)
}

//Stop : stop consumer
func (c *Consumer) Stop() {
	c.stopOnce.Do(c.stop)
}

//Wait : wait consumer quit
func (c *Consumer) Wait() {
	<-c.exitSignal
}

//ExitSignal : exit consumer signal
func (c *Consumer) ExitSignal() <-chan struct{} {
	return c.exitSignal
}

//start : start consumer in go routine
func (c *Consumer) start() {
	go func() {
		defer close(c.exitSignal)
		var timer *time.Timer
		for {
			c.process()
			if timer == nil {
				timer = time.NewTimer(c.tick)
			}
			select {
			case <-timer.C:
				timer.Reset(c.tick)
			case <-c.stopChan:
				c.submitWhenExit()
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
		}
	}()
}

//stop : stop consumer
func (c *Consumer) stop() {
	close(c.stopChan)
}

//process : process fetch/do msg/check submit
func (c *Consumer) process() {
	var mgs, err = c.readN()
	if err != nil {
		//fetch error, break process
		c.eh.ReadErrHandler.HandleErr(err)
		return
	}
	for _, msg := range mgs {
		c.loopDoMsg(msg)
	}
	c.submitCatch()
}

//submitCatch : submit with retry in case condition match
func (c *Consumer) submitCatch() {
	if c.consumedID <= c.submittedID {
		return
	}
	if !c.canSubmitCatch() {
		return
	}
	c.submitWithRetry()
}

//submitWhenExit : submit when exit
func (c *Consumer) submitWhenExit() {
	if c.consumedID <= c.submittedID {
		return
	}
	c.submitWithRetry()
}

//loopDoMsg : loop do msg
func (c *Consumer) loopDoMsg(msg *Msg) {
	for {
		var err = c.mh.Do(msg)
		if err != nil {
			c.eh.MsgErrHandler.HandleErr(err)
		} else {
			c.consumedID = msg.ID
			return
		}
	}
}

//readN : read n messages
func (c *Consumer) readN() ([]*Msg, error) {
	return getMgs(c.db, c.topicTbl, c.consumedID, c.n)
}

//canSubmitCatch : check can submit catch or not
func (c *Consumer) canSubmitCatch() bool {
	if c.consumedID-c.submittedID >= c.catchN {
		return true
	} else {
		var now = time.Now()
		var dur = now.Sub(c.submittedTS)
		if dur >= c.catchT {
			return true
		}
	}
	return false
}

//submitWithRetry : submit with retry
func (c *Consumer) submitWithRetry() {
	for i := 0; i < c.submitRetryNum; i++ {
		var ts = time.Now()
		var err = upsertClientCursor(c.db, c.cursorTbl, c.id, c.consumedID, ts)
		if err != nil {
			c.eh.SubmitErrHandler.HandleErr(err)
		} else {
			c.submittedID = c.consumedID
			c.submittedTS = ts
			return
		}
	}
}

//NewDB : new db
func NewDB(c *Cnf) (*gorm.DB, error) {
	var (
		db  *gorm.DB
		err error
	)
	if c.LogGorm {
		db, err = gormx.New(c.MySQLDsn.UseDefault(), gormx.WithLog())
	} else {
		db, err = gormx.New(c.MySQLDsn.UseDefault())
	}
	if err != nil {
		return nil, err
	}
	return db, nil
}
