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
					path, _ := os.MkdirTemp("", "ssdb")
					defer os.RemoveAll(path)

					config := &ssdb.Config{
						Path: path,
						Addr: ":7979",
					}

					s, err := ssdb.NewServer(config)
					if err != nil {
						log.Fatal(err)
					}
					defer s.Close()

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
