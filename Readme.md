# SQLite to Filecoin Backuper

A CLI app that periodically makes backups of your SQLite database and upload it to Filecoin.

## Building

```bash
go build ./cmd/tblbk
```

## Configuring

```bash
tblbk init 
```

Fill up `~/.tblbk/config.toml`:

```toml
[backuper]
  Dir = "backups"
  Frequency = 240
  EnableVacuum = true
  EnableCompression = true

[sinks]
  [sinks.estuary]
    Enabled = false
    ApiKey = ""
  [sinks.fvm]
    Enabled = false
    Web3StorageToken = ""
    ChainID = 0
    Contract = "0x0000000000000000000000000000000000000000"
```

## Running

```bash
tblbk daemon <DB_PATH>
```

## Todos

- [] Estuary integration
- [] Tests
- [] Goreleaser
- [] Validate deal request parameters
