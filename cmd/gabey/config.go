package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"unicode"

	"gopkg.in/urfave/cli.v1"

	"github.com/naoina/toml"
	"github.com/abeychain/go-abey/cmd/utils"
	"github.com/abeychain/go-abey/crypto"
	"github.com/abeychain/go-abey/abey"
	"github.com/abeychain/go-abey/node"
	"github.com/abeychain/go-abey/params"
)

var (
	dumpConfigCommand = cli.Command{
		Action:      utils.MigrateFlags(dumpConfig),
		Name:        "dumpconfig",
		Usage:       "Show configuration values",
		ArgsUsage:   "",
		Flags:       append(append(nodeFlags, rpcFlags...)),
		Category:    "MISCELLANEOUS COMMANDS",
		Description: `The dumpconfig command shows configuration values.`,
	}

	configFileFlag = cli.StringFlag{
		Name:  "config",
		Usage: "TOML configuration file",
	}
)

// These settings ensure that TOML keys use the same names as Go struct fields.
var tomlSettings = toml.Config{
	NormFieldName: func(rt reflect.Type, key string) string {
		return key
	},
	FieldToKey: func(rt reflect.Type, field string) string {
		return field
	},
	MissingField: func(rt reflect.Type, field string) error {
		link := ""
		if unicode.IsUpper(rune(rt.Name()[0])) && rt.PkgPath() != "main" {
			link = fmt.Sprintf(", see https://godoc.org/%s#%s for available fields", rt.PkgPath(), rt.Name())
		}
		return fmt.Errorf("field '%s' is not defined in %s%s", field, rt.String(), link)
	},
}

type abeystatsConfig struct {
	URL string `toml:",omitempty"`
}

type gethConfig struct {
	Abey      abey.Config
	Node       node.Config
	Abeystats abeystatsConfig
}

func loadConfig(file string, cfg *gethConfig) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	err = tomlSettings.NewDecoder(bufio.NewReader(f)).Decode(cfg)
	// Add file name to errors that have a line number.
	if _, ok := err.(*toml.LineError); ok {
		err = errors.New(file + ", " + err.Error())
	}
	return err
}

func defaultNodeConfig() node.Config {
	cfg := node.DefaultConfig
	cfg.Name = clientIdentifier
	cfg.Version = params.VersionWithCommit(gitCommit, gitDate)
	cfg.HTTPModules = append(cfg.HTTPModules, "abey", "eth", "impawn", "shh")
	cfg.WSModules = append(cfg.WSModules, "abey")
	cfg.IPCPath = "gabey.ipc"
	return cfg
}

func makeConfigNode(ctx *cli.Context) (*node.Node, gethConfig) {
	// Load defaults.
	cfg := gethConfig{
		Abey:     abey.DefaultConfig,
		Node:      defaultNodeConfig(),
	}
	if ctx.GlobalBool(utils.SingleNodeFlag.Name) {
		// set abeyconfig
		prikey, _ := crypto.HexToECDSA("229ca04fb83ec698296037c7d2b04a731905df53b96c260555cbeed9e4c64036")
		cfg.Abey.PrivateKey = prikey
		cfg.Abey.CommitteeKey = crypto.FromECDSA(prikey)

		//cfg.Abey.MineFruit = true
		cfg.Abey.Mine = true
		cfg.Abey.Etherbase = crypto.PubkeyToAddress(prikey.PublicKey)
		//cfg.Abey.NetworkId =400
		//set node config
		cfg.Node.HTTPPort = 8888
		cfg.Node.HTTPHost = "127.0.0.1"
		cfg.Node.HTTPModules = []string{"db", "abey", "net", "web3", "personal", "admin", "miner", "eth"}

		ctx.GlobalSet("datadir", "./data")
	}
	// Load config file.
	if file := ctx.GlobalString(configFileFlag.Name); file != "" {
		if err := loadConfig(file, &cfg); err != nil {
			utils.Fatalf("%v", err)
		}
	}

	// Apply flags.
	utils.SetNodeConfig(ctx, &cfg.Node)
	stack, err := node.New(&cfg.Node)
	if err != nil {
		utils.Fatalf("Failed to create the protocol stack: %v", err)
	}
	utils.SetAbeychainConfig(ctx, stack, &cfg.Abey)
	if ctx.GlobalIsSet(utils.AbeystatsURLFlag.Name) {
		cfg.Abeystats.URL = ctx.GlobalString(utils.AbeystatsURLFlag.Name)
	}

	return stack, cfg
}

func makeFullNode(ctx *cli.Context) *node.Node {
	stack, cfg := makeConfigNode(ctx)

	utils.RegisterAbeyService(stack, &cfg.Abey)

	// Add the Abeychain Stats daemon if requested.
	if cfg.Abeystats.URL != "" {
		utils.RegisterAbeystatsService(stack, cfg.Abeystats.URL)
	}
	return stack
}

// dumpConfig is the dumpconfig command.
func dumpConfig(ctx *cli.Context) error {
	_, cfg := makeConfigNode(ctx)
	comment := ""

	if cfg.Abey.Genesis != nil {
		cfg.Abey.Genesis = nil
		comment += "# Note: this config doesn't contain the genesis block.\n\n"
	}

	out, err := tomlSettings.Marshal(&cfg)
	if err != nil {
		return err
	}
	io.WriteString(os.Stdout, comment)
	os.Stdout.Write(out)
	return nil
}
