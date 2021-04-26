package main

import (
	"encoding/hex"
	"fmt"

	"github.com/abeychain/go-abey/cmd/utils"
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/crypto"

	"gopkg.in/urfave/cli.v1"
)

var (
	generateCommand = cli.Command{
		Name:      "generate",
		Usage:     "Generate new key item",
		ArgsUsage: "",
		Description: `
Generate a new key item.
`,
		Flags: []cli.Flag{
			cli.IntFlag{
				Name:  "sum",
				Usage: "key info count",
				Value: 1,
			},
		},
		Action: func(ctx *cli.Context) error {
			count := ctx.Int("sum")
			if count <= 0 || count > 100 {
				count = 100
			}
			makeAddress(count)

			return nil
		},
	}

	convertCommand = cli.Command{
		Name:        "convert",
		Usage:       "Convert between abey address and hex address",
		Description: "Convert between abey address and hex address",
		Subcommands: []cli.Command{
			{
				Name:  "hex",
				Usage: "Convert hex address to abey address",
				Action: func(c *cli.Context) error {
					hexAddress := c.Args().First()
					if hexAddress == "" {
						return cli.NewExitError("please check the input args", -1)
					}
					fmt.Println("abey address: ", HexToAbey(hexAddress))
					return nil
				},
			},
			{
				Name:  "abey",
				Usage: "Convert abey address to hex address",
				Action: func(c *cli.Context) error {
					abeyAddress := c.Args().First()
					if abeyAddress == "" {
						return cli.NewExitError("please check the input args", -1)
					}
					hexAddress, err := AbeyToHex(abeyAddress)
					if err != nil {
						return cli.NewExitError(err.Error(), -1)
					}
					fmt.Println("hex address: ", hexAddress)
					return nil
				},
			},
		},
	}
)

func makeAddress(count int) {
	for i := 0; i < count; i++ {
		if privateKey, err := crypto.GenerateKey(); err != nil {
			utils.Fatalf("Error GenerateKey: %v", err)
		} else {
			fmt.Println("private key:", hex.EncodeToString(crypto.FromECDSA(privateKey)))
			fmt.Println("public key:", hex.EncodeToString(crypto.FromECDSAPub(&privateKey.PublicKey)))
			addr := crypto.PubkeyToAddress(privateKey.PublicKey)
			//fmt.Println("address-CV:", addr.String())
			//fmt.Println("address-0x:", addr.StringToVC())
			fmt.Println("address-0x: ", addr.String())
			fmt.Println("address-abey: ", HexToAbey(addr.String()))
			fmt.Println("-------------------------------------------------------")
		}
	}
}

func HexToAbey(hex string) string {
	return common.HexToAddress(hex).StringToAbey()
}

func AbeyToHex(abey string) (string, error) {
	a := common.Address{}
	if err := a.FromAbeyString(abey); err != nil {
		return "", err
	}
	return a.Hex(), nil
}
