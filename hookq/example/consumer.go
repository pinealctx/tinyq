package main

import (
	"github.com/pinealctx/tinyq/hookq"
	"github.com/urfave/cli/v2"
	"github.com/vmihailenco/msgpack/v5"
	"log"
)

type MsgHandler struct {
}

func (m MsgHandler) Do(msg *hookq.Msg) error {
	log.Println("id:", msg.ID)
	log.Println("protocol:", msg.P)
	var a OrderAction
	var err = msgpack.Unmarshal(msg.Data, &a)
	if err != nil {
		return err
	}
	log.Printf("msg:{id:%d, state:%d}\n", a.ID, a.State)
	return nil
}

type ErrorHandler struct {
	category string
}

func NewErrorHandler(cat string) *ErrorHandler {
	return &ErrorHandler{category: cat}
}

func (m *ErrorHandler) HandleErr(err error) {
	log.Println("cat:", m.category, "error:", err)
	panic(err)
}

//Consumer相关命令
var consumerCmd = &cli.Command{
	Name:  "consumer",
	Usage: "consumer",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "id",
			Usage: "client id",
			Value: "",
		},
		&cli.IntFlag{
			Name:  "n",
			Usage: "fetch number",
			Value: 0,
		},
		&cli.DurationFlag{
			Name:  "tick",
			Usage: "tick",
			Value: 0,
		},
		&cli.Int64Flag{
			Name:  "catchN",
			Usage: "catch number",
			Value: 0,
		},
		&cli.DurationFlag{
			Name:  "catchT",
			Usage: "catch time duration",
			Value: 0,
		},
	},
	Action: consumerMsg,
}

func consumerMsg(c *cli.Context) error {
	var cnf, err = loadConsumerCnf(c)
	if err != nil {
		return err
	}
	var consumer *hookq.Consumer
	consumer, err = hookq.NewConsumer(cnf, MsgHandler{}, &hookq.ErrHandler{
		ReadErrHandler:   NewErrorHandler("read"),
		SubmitErrHandler: NewErrorHandler("submit"),
		MsgErrHandler:    NewErrorHandler("msg"),
	})
	if err != nil {
		return err
	}
	consumer.Start()
	consumer.Wait()
	return nil
}

func loadConsumerCnf(c *cli.Context) (*hookq.Cnf, error) {
	var cnf, err = loadCnf(c)
	if err != nil {
		return nil, err
	}
	var (
		id     = c.String("id")
		n      = c.Int("n")
		tick   = c.Duration("tick")
		catchN = c.Int64("catchN")
		catchT = c.Duration("catchT")
	)

	if id != "" {
		cnf.ID = id
	}
	if n != 0 {
		cnf.N = n
	}
	if tick != 0 {
		cnf.Tick.Duration = tick
	}
	if catchN != 0 {
		cnf.CatchN = catchN
	}
	if catchT != 0 {
		cnf.CatchT.Duration = catchT
	}
	return cnf, nil
}
