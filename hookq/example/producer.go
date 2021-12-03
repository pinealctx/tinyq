package main

import (
	"github.com/pinealctx/neptune/idgen/snowflake"
	"github.com/pinealctx/neptune/remap"
	"github.com/pinealctx/tinyq/hookq"
	"github.com/urfave/cli/v2"
	"gorm.io/gorm"
)

//Producer命令
var producerCmd = &cli.Command{
	Name:  "producer",
	Usage: "producer",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "lb",
			Usage: "logic db table name",
		},
		&cli.IntFlag{
			Name:  "n",
			Usage: "produce number",
			Value: 5,
		},
	},
	Action: produceMsg,
}

func produceMsg(c *cli.Context) error {
	var cnf, err = loadCnf(c)
	if err != nil {
		return err
	}
	var db *gorm.DB
	db, err = hookq.NewDB(cnf)
	if err != nil {
		return err
	}
	var idGen snowflake.Node
	idGen, err = snowflake.NewNode(1, 0)
	if err != nil {
		return err
	}

	var (
		producer *hookq.Producer
		n        = c.Int("n")
		lb       = c.String("lb")
	)

	producer, err = hookq.NewProducer(db, cnf.TopicTbl)
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		var id = idGen.Generate()
		var index = remap.XXHash(id)
		var o = &OrderState{
			ID:    id,
			State: int32(index % 4),
		}
		var proc = NewOrderStateProc(o, lb)
		err = producer.WriteHook(db, proc)
		if err != nil {
			return err
		}
	}
	return nil
}
