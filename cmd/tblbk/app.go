package main

import (
	"fmt"

	"github.com/brunocalza/tblbk/pkg/backup"
	"github.com/brunocalza/tblbk/pkg/sinks/fvm"
)

type App struct {
	scheduler *backup.Scheduler
	sink      *fvm.FVMSink
}

func NewApp(cfg *Config, dbPath string) (*App, error) {
	ch := make(chan backup.BackupResult)

	backupScheduler, err := backup.NewScheduler(
		cfg.Backuper.Frequency,
		ch,
		backup.BackuperOptions{
			SourcePath: dbPath,
			BackupDir:  cfg.Backuper.Dir,
			Opts: []backup.Option{
				backup.WithCompression(cfg.Backuper.EnableCompression),
				backup.WithVacuum(cfg.Backuper.EnableVacuum),
				// backup.WithPruning(cfg.Pruning.Enabled, cfg.Pruning.KeepFiles),
			},
		})
	if err != nil {
		return nil, fmt.Errorf("creating backup scheduler: %s", err)
	}

	fvmCfg := cfg.Sinks["fvm"].(FVM)
	sink := fvm.NewFVMSink(ch, &fvm.Config{
		ApiToken:   fvmCfg.Web3StorageToken,
		PrivateKey: fvmCfg.PrivateKey,
		Contract:   fvmCfg.Contract,
		Gateway:    fvmCfg.Gateway,
		ChainID:    fvmCfg.ChainID,
	})

	return &App{
		scheduler: backupScheduler,
		sink:      sink,
	}, nil
}

func (app *App) Run() {
	go func() { app.scheduler.Run() }() // don't forget to close
	go func() { app.sink.Start() }()
}
