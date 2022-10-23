package main

import (
	"log"
	"os"

	"github.com/decimalbell/go-ssdb"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Commands: []*cli.Command{
			{
				Name:  "server",
				Usage: "server",
				Action: func(cCtx *cli.Context) error {
					opts := &ssdb.Options{
						Addr: ":7979",
					}
					s := ssdb.NewServer(opts)
					log.Fatal(s.ListenAndServe())

					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
