package hookq

import (
	"fmt"
	"github.com/pinealctx/neptune/store/gormx"
	"gorm.io/gorm"
	"time"
)

const (
	//MsgPack current support msgpack
	MsgPack Protocol = 1
)

//Protocol message protocol define
type Protocol int32

//String : string protocol value
func (p Protocol) String() string {
	if p == MsgPack {
		return "msgpack"
	}
	return fmt.Sprintf("unkonwn:%d", p)
}

//Msg message define
type Msg struct {
	//ID message id
	ID int64 `gorm:"column:id"`
	//P message protocol
	P Protocol `gorm:"column:p"`
	//Data -- max size 16M
	Data []byte `gorm:"column:data"`
	//CreatedAt -- created time
	CreatedAt time.Time `gorm:"column:created_at"`
}

//IDW : id wrapper
type IDW struct {
	//ID message id
	ID int64 `gorm:"column:id"`
}

//Cursor message consumer cursor
type Cursor struct {
	//ClientID client id
	ClientID string `gorm:"column:cid"`
	//ConsumedID client consumed id
	ConsumedID int64 `gorm:"column:sid"`
	//UpdatedAt updated time
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

//add message
func addMsg(db *gorm.DB, tlb string, msg *Msg) error {
	return db.Table(tlb).Create(msg).Error
}

//get messages
func getMgs(db *gorm.DB, tlb string, offsetID int64, n int) ([]*Msg, error) {
	var ms []*Msg
	var err = db.Table(tlb).Where("id>?", offsetID).Limit(n).Find(&ms).Error
	if err != nil {
		return nil, err
	}
	return ms, nil
}

//get max msg id
func getMaxMsgID(db *gorm.DB, tlb string) (int64, error) {
	var ids []IDW
	var err = db.Table(tlb).Order("id desc").Limit(1).Find(&ids).Error
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	return ids[0].ID, nil
}

//get client cursor
func getClientCursor(db *gorm.DB, tlb string, cid string) (int64, time.Time, error) {
	var cur Cursor
	var err = db.Table(tlb).Where("cid=?", cid).First(&cur).Error
	if err != nil {
		var ts = time.Now()
		if gormx.IsNotFoundErr(err) {
			return 0, ts, nil
		}
		return 0, ts, err
	}
	return cur.ConsumedID, cur.UpdatedAt, nil
}

//upsert client cursor
func upsertClientCursor(db *gorm.DB, tlb string, cid string, sid int64, ts time.Time) error {
	var txn = db.Table(tlb).Where("cid=?", cid).Updates(
		map[string]interface{}{
			"sid":        sid,
			"updated_at": ts,
		})
	var err = txn.Error
	if err != nil {
		return err
	}
	var affect = txn.RowsAffected
	if affect != 1 {
		//try to insert
		err = db.Table(tlb).Create(&Cursor{
			ClientID:   cid,
			ConsumedID: sid,
			UpdatedAt:  ts,
		}).Error
		if err != nil {
			if !gormx.IsDupError(err) {
				return err
			}
			return nil
		}
	}
	return nil
}
