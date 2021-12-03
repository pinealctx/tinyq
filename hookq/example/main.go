package main

import (
	"fmt"
	"github.com/pinealctx/tinyq/hookq"
	"github.com/urfave/cli/v2"
	"os"
)

const (
	nameCnfFile = "cnf"
)

func main() {
	var app = cli.NewApp()
	app.Name = "message queue example"
	app.Usage = "message queue example"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  nameCnfFile,
			Usage: "Specify environment config file",
			Value: "cnf.json",
		},
	}
	app.Commands = cli.Commands{
		producerCmd,
		consumerCmd,
	}
	var err = app.Run(os.Args)
	if err != nil {
		fmt.Printf("run error: %+v\n", err)
	}
}

//loadCnf : load config
func loadCnf(c *cli.Context) (*hookq.Cnf, error) {
	return hookq.LoadCnf(c.String(nameCnfFile))
}
