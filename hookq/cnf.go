package hookq

import (
	"errors"
	"github.com/pinealctx/neptune/jsonx"
	"github.com/pinealctx/neptune/store/gormx"
	"github.com/pinealctx/neptune/tex"
	"time"
)

const (
	nMin = 1
	nMax = 10000

	tickMin = time.Millisecond
	tickMax = time.Hour

	catchNMin = 1
	catchNMax = 10000

	catchTMin = time.Second
	catchTMax = time.Hour * 10

	submitRetryMin = 1
	submitRetryMax = 10000
)

//Cnf : consumer config
type Cnf struct {
	//mysql dsn config
	MySQLDsn *gormx.Dsn `json:"mysql"`
	//LogGorm -- log gorm or not
	LogGorm bool `json:"logGorm"`
	//topic tbl string
	TopicTbl string `json:"topicTbl"`
	//cursor tbl string
	CursorTbl string `json:"cursorTbl"`

	//client id
	ID string `json:"id"`

	//n -- read n in each fetch
	N int `json:"n"`
	//tick -- time duration
	Tick tex.JsDuration `json:"tick"`

	// -- after msg consumed count to submit cursor
	CatchN int64 `json:"catchN"`
	// -- after msg consumed time to submit cursor
	CatchT tex.JsDuration `json:"catchT"`
	// -- submit retry
	SubmitRetryNum int `json:"submitRetryNum"`
}

//Normalize : normalize config
func (c *Cnf) Normalize() error {
	if c.TopicTbl == "" {
		return errors.New("cnf.topic.tbl.name.empty")
	}
	if c.CursorTbl == "" {
		return errors.New("cnf.cursor.tbl.name.empty")
	}
	if c.ID == "" {
		return errors.New("cnf.cli.id.empty")
	}

	if c.N < nMin {
		c.N = nMin
	}
	if c.N > nMax {
		c.N = nMax
	}

	if c.Tick.Duration < tickMin {
		c.Tick.Duration = tickMin
	}
	if c.Tick.Duration > tickMax {
		c.Tick.Duration = tickMax
	}

	if c.CatchN < catchNMin {
		c.CatchN = catchNMin
	}
	if c.CatchN > catchNMax {
		c.CatchN = catchNMax
	}

	if c.CatchT.Duration < catchTMin {
		c.CatchT.Duration = catchTMin
	}
	if c.CatchT.Duration > catchTMax {
		c.CatchT.Duration = catchTMax
	}

	if c.SubmitRetryNum < submitRetryMin {
		c.SubmitRetryNum = submitRetryMin
	}
	if c.SubmitRetryNum > submitRetryMax {
		c.SubmitRetryNum = submitRetryMax
	}

	return nil
}

//LoadCnf : load cnf
func LoadCnf(fName string) (*Cnf, error) {
	var cnf = &Cnf{}
	var err = jsonx.LoadJSONFile2Obj(fName, cnf)
	if err != nil {
		return nil, err
	}
	err = cnf.Normalize()
	if err != nil {
		return nil, err
	}
	return cnf, nil
}
