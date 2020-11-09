package lib

// Types in this file are extracted from lotus to break dependency which is extraneous to maintain
// and blocks ent development on lotus integration of latest changes before testing out migrations
import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	proof2 "github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	cid "github.com/ipfs/go-cid"
)

// From lotus/chain/types/blockheader.go and electionproof.go

type Ticket struct {
	VRFProof []byte
}

type BeaconEntry struct {
	Round uint64
	Data  []byte
}

type ElectionProof struct {
	WinCount int64
	VRFProof []byte
}

type BlockHeader struct {
	Miner address.Address // 0

	Ticket *Ticket // 1

	ElectionProof *ElectionProof // 2

	BeaconEntries []BeaconEntry // 3

	WinPoStProof []proof2.PoStProof // 4

	Parents []cid.Cid // 5

	ParentWeight big.Int // 6

	Height abi.ChainEpoch // 7

	ParentStateRoot cid.Cid // 8

	ParentMessageReceipts cid.Cid // 8

	Messages cid.Cid // 10

	BLSAggregate *crypto.Signature // 11

	Timestamp uint64 // 12

	BlockSig *crypto.Signature // 13

	ForkSignaling uint64 // 14

	// ParentBaseFee is the base fee after executing parent tipset
	ParentBaseFee abi.TokenAmount // 15

	// internal
	validated bool // true if the signature has been validated
}

// func DecodeBlock(b []byte) (*BlockHeader, error) {
// 	var blk BlockHeader
// 	if err := blk.UnmarshalCBOR(bytes.NewReader(b)); err != nil {
// 		return nil, err
// 	}

// 	return &blk, nil
// }

// From lotus/chain/types/state.go

// StateTreeVersion is the version of the state tree itself, independent of the
// network version or the actors version.
type StateTreeVersion uint64

const (
	// StateTreeVersion0 corresponds to actors < v2.
	StateTreeVersion0 StateTreeVersion = iota
	// StateTreeVersion1 corresponds to actors >= v2.
	StateTreeVersion1
)

type StateRoot struct {
	// State tree version.
	Version StateTreeVersion
	// Actors tree. The structure depends on the state root version.
	Actors cid.Cid
	// Info. The structure depends on the state root version.
	Info cid.Cid
}
