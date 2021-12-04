package main

import (
	"github.com/pinealctx/tinyq/hookq"
	"github.com/urfave/cli/v2"
	"github.com/vmihailenco/msgpack/v5"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type MsgHandler struct {
	panicMode bool
}

func NewMsgHandler(panicMode bool) hookq.MsgHandler {
	return &MsgHandler{panicMode: panicMode}
}

func (m *MsgHandler) Do(msg *hookq.Msg) error {
	log.Println("id:", msg.ID)
	log.Println("protocol:", msg.P)
	var a OrderAction
	var err = msgpack.Unmarshal(msg.Data, &a)
	if err != nil {
		return err
	}
	log.Printf("msg:{id:%d, state:%d}\n", a.ID, a.State)
	if m.panicMode {
		panic("self.panic")
	}
	return nil
}

type ErrorHandler struct {
	category string
}

func NewErrorHandler(cat string) hookq.ErrDo {
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
		&cli.StringFlag{
			Name:  "m",
			Usage: "mode -- n(tiny mode)/p(panic mode)/w(whole mode)",
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
	var mode = c.String("m")
	var msgHandler hookq.MsgHandler
	if mode == "p" {
		msgHandler = NewMsgHandler(true)
	} else {
		msgHandler = NewMsgHandler(false)
	}

	consumer, err = hookq.NewConsumer(cnf, msgHandler, &hookq.ErrHandler{
		ReadErrHandler:   NewErrorHandler("read"),
		SubmitErrHandler: NewErrorHandler("submit"),
		MsgErrHandler:    NewErrorHandler("msg"),
	})
	if err != nil {
		return err
	}
	consumer.Start()

	switch mode {
	case "p", "w":
		var signalChan = make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-signalChan:
			log.Println("received.exit.ctrl-c")
			consumer.Stop()
			consumer.Wait()
			log.Println("consumer.exit.by.signal")
			return nil
		case <-consumer.ExitSignal():
			log.Println("consumer.exit.by.internal.error")
			return nil
		}
	default:
		consumer.Wait()
		log.Println("consumer.default.quit")
		return nil
	}
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
