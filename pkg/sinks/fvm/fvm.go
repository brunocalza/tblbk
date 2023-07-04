package fvm

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"fmt"
	"io"
	"math/big"
	"net/url"
	"os"
	"path"

	"github.com/brunocalza/tblbk/pkg/backup"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	logger "github.com/rs/zerolog/log"
	"github.com/tech-greedy/go-generate-car/util"
	"github.com/web3-storage/go-w3s-client"
)

const BufSize = (4 << 20) / 128 * 127

const (
	// DealStartOffset is the time in epochs the deal will be activated from current epoch.
	DealStartOffset = 3000

	// DealDuration means for how long the deal will be active.
	DealDuration = 1000000
)

var log = logger.With().Str("sink", "fvm").Logger()

// FVMSink implements a sink that make deals on Filecoin by calling a Smart Contract.
type FVMSink struct {
	ch  chan backup.BackupResult
	cfg *Config
}

// NewFVMSink creates a new FVMSink.
func NewFVMSink(ch chan backup.BackupResult, cfg *Config) *FVMSink {
	return &FVMSink{
		ch:  ch,
		cfg: cfg,
	}
}

func (s *FVMSink) Start() {
	for backupResult := range s.ch {
		deal, err := s.generateCar(backupResult)
		if err != nil {
			log.Error().Err(err).Msg("generate car")
			continue
		}

		deal, err = s.uploadCar(deal)
		if err != nil {
			log.Error().Err(err).Msg("upload car")
			continue
		}

		log.Info().
			Str("piece_cid", deal.PieceCid).
			Str("payload_cid", deal.DataCid).
			Str("location_ref", deal.LocationRef).
			Msg("deal is ready")

		tx, err := s.makeDeal(deal)
		if err != nil {
			log.Error().Err(err).Msg("make deal")
			continue
		}

		log.Info().
			Str("tx", tx.Hash().Hex()).
			Msg("deal requested")
	}
}

func (s *FVMSink) generateCar(res backup.BackupResult) (DealInfo, error) {
	stat, err := os.Stat(res.Path)
	if err != nil {
		return DealInfo{}, err
	}

	input := []util.Finfo{
		{
			Path:  res.Path,
			Size:  stat.Size(),
			Start: 0,
			End:   stat.Size(),
		},
	}

	outFilename := uuid.New().String() + ".car"
	outPath := path.Join("backups", outFilename)
	carF, err := os.Create(outPath)
	if err != nil {
		return DealInfo{}, err
	}
	cp := new(commp.Calc)
	writer := bufio.NewWriterSize(io.MultiWriter(carF, cp), BufSize)
	ipld, cid, cidMap, err := util.GenerateCar(context.Background(), input, "", "", writer)
	if err != nil {
		return DealInfo{}, err
	}
	err = writer.Flush()
	if err != nil {
		return DealInfo{}, err
	}
	err = carF.Close()
	if err != nil {
		return DealInfo{}, err
	}
	rawCommP, pieceSize, err := cp.Digest()
	if err != nil {
		return DealInfo{}, err
	}

	commCid, err := commcid.DataCommitmentV1ToCID(rawCommP)
	if err != nil {
		return DealInfo{}, err
	}

	return DealInfo{
		Ipld:      ipld,
		DataCid:   cid,
		PieceCid:  commCid.String(),
		PieceSize: pieceSize,
		CidMap:    cidMap,
		Path:      outPath,
	}, nil
}

func (s *FVMSink) uploadCar(deal DealInfo) (DealInfo, error) {
	client, err := w3s.NewClient(w3s.WithToken(s.cfg.ApiToken))
	if err != nil {
		return DealInfo{}, fmt.Errorf("w3s new client: %s", err)
	}

	file, err := os.Open(deal.Path)
	if err != nil {
		return DealInfo{}, fmt.Errorf("open deal file: %s", err)
	}

	basename := path.Base(file.Name())
	cid, err := client.Put(context.Background(), file)
	if err != nil {
		return DealInfo{}, fmt.Errorf("uploading to web3 storage: %s", err)
	}

	deal.LocationRef = fmt.Sprintf("https://%s.ipfs.dweb.link/%s", cid.String(), basename)
	return deal, nil
}

func (s *FVMSink) makeDeal(deal DealInfo) (*types.Transaction, error) {
	conn, err := ethclient.Dial(s.cfg.Gateway.String())
	if err != nil {
		return nil, fmt.Errorf("eth client dial: %s", err)
	}

	latestBlock, err := conn.BlockByNumber(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("block by number: %s", err)
	}

	contract, err := NewContract(s.cfg.Contract, conn)
	if err != nil {
		return nil, fmt.Errorf("new contract: %s", err)
	}

	pieceCid, err := cid.Decode(deal.PieceCid)
	if err != nil {
		return nil, fmt.Errorf("decoding cid: %s", err)
	}

	dealRequest := DealRequest{
		PieceCid:             pieceCid.Bytes(),
		PieceSize:            deal.PieceSize,
		VerifiedDeal:         false,
		Label:                deal.DataCid,
		StartEpoch:           latestBlock.Header().Number.Int64() + DealStartOffset,
		EndEpoch:             latestBlock.Header().Number.Int64() + DealStartOffset + DealDuration,
		StoragePricePerEpoch: big.NewInt(300),
		ProviderCollateral:   big.NewInt(0),
		ClientCollateral:     big.NewInt(0),
		ExtraParamsVersion:   1,
		ExtraParams: ExtraParamsV1{
			LocationRef:        deal.LocationRef,
			CarSize:            deal.Ipld.Size,
			SkipIpniAnnounce:   false,
			RemoveUnsealedCopy: false,
		},
	}

	auth, err := bind.NewKeyedTransactorWithChainID(s.cfg.PrivateKey, big.NewInt(s.cfg.ChainID))
	if err != nil {
		return nil, fmt.Errorf("new keyed transactor with chain id: %s", err)
	}

	tx, err := contract.MakeDealProposal(auth, dealRequest)
	if err != nil {
		return nil, fmt.Errorf("make deal proposal: %s", err)
	}

	return tx, nil
}

// DealInfo contains information about deal.
type DealInfo struct {
	Ipld        *util.FsNode
	DataCid     string
	PieceCid    string
	PieceSize   uint64
	CidMap      map[string]util.CidMapValue
	Path        string
	LocationRef string
}

// Config contains configuration parameters for FVM sink.
type Config struct {
	ApiToken   string
	PrivateKey *ecdsa.PrivateKey
	Contract   common.Address
	ChainID    int64
	Gateway    *url.URL
}
