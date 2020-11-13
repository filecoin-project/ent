package lib

import (
	"bytes"
	"context"

	block "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	mh "github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

// extracted from lotus/chain/vm/vm.go

func linksForObj(blk block.Block, cb func(cid.Cid)) error {
	switch blk.Cid().Prefix().Codec {
	case cid.DagCBOR:
		err := cbg.ScanForLinks(bytes.NewReader(blk.RawData()), cb)
		if err != nil {
			return xerrors.Errorf("cbg.ScanForLinks: %w", err)
		}
		return nil
	case cid.Raw:
		// We implicitly have all children of raw blocks.
		return nil
	default:
		return xerrors.Errorf("vm flush copy method only supports dag cbor")
	}
}

func BlockstoreCopy(ctx context.Context, from, to blockstore.Blockstore, root cid.Cid) error {
	var numBlocks int
	var totalCopySize int

	var batch []block.Block
	batchCp := func(blk block.Block) error {
		numBlocks++
		totalCopySize += len(blk.RawData())

		batch = append(batch, blk)
		if len(batch) > 100 {
			if err := to.PutMany(batch); err != nil {
				return xerrors.Errorf("batch put in copy: %w", err)
			}
			batch = batch[:0]
		}
		return nil
	}

	if err := copyRec(from, to, root, batchCp); err != nil {
		return xerrors.Errorf("copyRec: %w", err)
	}

	if len(batch) > 0 {
		if err := to.PutMany(batch); err != nil {
			return xerrors.Errorf("batch put in copy: %w", err)
		}
	}

	return nil
}

func copyRec(from, to blockstore.Blockstore, root cid.Cid, cp func(block.Block) error) error {
	if root.Prefix().MhType == 0 {
		// identity cid, skip
		return nil
	}

	blk, err := from.Get(root)
	if err != nil {
		return xerrors.Errorf("get %s failed: %w", root, err)
	}

	var lerr error
	err = linksForObj(blk, func(link cid.Cid) {
		if lerr != nil {
			// Theres no erorr return on linksForObj callback :(
			return
		}

		prefix := link.Prefix()
		if prefix.Codec == cid.FilCommitmentSealed || prefix.Codec == cid.FilCommitmentUnsealed {
			return
		}

		// We always have blocks inlined into CIDs, but we may not have their children.
		if prefix.MhType == mh.IDENTITY {
			// Unless the inlined block has no children.
			if prefix.Codec == cid.Raw {
				return
			}
		} else {
			// If we have an object, we already have its children, skip the object.
			has, err := to.Has(link)
			if err != nil {
				lerr = xerrors.Errorf("has: %w", err)
				return
			}
			if has {
				return
			}
		}

		if err := copyRec(from, to, link, cp); err != nil {
			lerr = err
			return
		}
	})
	if err != nil {
		return xerrors.Errorf("linksForObj (%x): %w", blk.RawData(), err)
	}
	if lerr != nil {
		return lerr
	}

	if err := cp(blk); err != nil {
		return xerrors.Errorf("copy: %w", err)
	}
	return nil
}

// extracted from /lotus/lib/blockstore/blockstore.go

// NewTemporary returns a temporary blockstore.
func NewTemporary() blockstore.Blockstore {
	return NewBlockstore(ds.NewMapDatastore())
}

// NewTemporarySync returns a thread-safe temporary blockstore.
func NewTemporarySync() blockstore.Blockstore {
	return NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
}

// WrapIDStore wraps the underlying blockstore in an "identity" blockstore.
func WrapIDStore(bstore blockstore.Blockstore) blockstore.Blockstore {
	return blockstore.NewIdStore(bstore)
}

// NewBlockstore creates a new blockstore wrapped by the given datastore.
func NewBlockstore(dstore ds.Batching) blockstore.Blockstore {
	return WrapIDStore(blockstore.NewBlockstore(dstore))
}
