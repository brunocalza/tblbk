package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

func main() {
	cliApp := &cli.App{
		Name:  "tblk",
		Usage: "tblk lets backups your SQLite database periodically in Filecoin",
		Commands: []*cli.Command{
			initCmd,
			daemonCmd,
		},
	}

	if err := cliApp.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

var initCmd = &cli.Command{
	Name:  "init",
	Usage: "init the config directory",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "dir",
			Usage: "The directory where config will be stored (default: $HOME)",
		},
	},
	Action: func(cCtx *cli.Context) error {
		dir, err := defaultConfigLocation(cCtx)
		if err != nil {
			return fmt.Errorf("default config location: %s", err)
		}

		cfg := DefaultConfig()
		f, err := os.Create(path.Join(dir, "config.toml"))
		if err != nil {
			return fmt.Errorf("os create: %s", err)
		}

		if err := toml.NewEncoder(f).Encode(cfg); err != nil {
			return fmt.Errorf("encode: %s", err)
		}

		return nil
	},
}

var daemonCmd = &cli.Command{
	Name:  "daemon",
	Usage: "runs a the backuper as a daemon",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "dir",
			Usage: "The directory where config is located (default: $HOME)",
		},
	},
	Action: func(cCtx *cli.Context) error {
		dir, err := defaultConfigLocation(cCtx)
		if err != nil {
			return fmt.Errorf("default config location: %s", err)
		}

		cfg, err := setupConfig(path.Join(dir, "config.toml"))
		if err != nil {
			return fmt.Errorf("setup config: %s", err)
		}

		firstArg := cCtx.Args().First()
		if firstArg == "" {
			return errors.New("missing database path")
		}

		app, err := NewApp(cfg, firstArg)
		if err != nil {
			log.Fatal(err)
		}

		app.Run()

		done := make(chan os.Signal, 1)
		signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
		<-done

		return nil
	},
}

func defaultConfigLocation(cCtx *cli.Context) (string, error) {
	var dir string
	dir = cCtx.String("dir")
	if dir == "" {
		// the default directory is home
		var err error
		dir, err = homedir.Dir()
		if err != nil {
			return "", fmt.Errorf("home dir: %s", err)
		}

		dir = path.Join(dir, ".tblk")
	}

	return dir, nil
}
