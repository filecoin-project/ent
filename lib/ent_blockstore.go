package lib

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"
)

// EntBlockstore reads from the lotus datastore and writes to the ent datastore
type EntBlockstore struct {
	read  blockstore.Blockstore
	write blockstore.Blockstore
}

func NewEntBlockstore(readLotusPath, writeEntPath string) (*EntBlockstore, error) {
	// load lotus chain datastore
	lotusExpPath, err := homedir.Expand(lotusPath)
	if err != nil {
		return nil, err
	}
	lotusDS, err := chainBadgerDs(lotusExpPath)
	if err != nil {
		return nil, err
	}
	entExpPath, err := homedir.Expand(entChainPath)
	if err != nil {
		return nil, err
	}
	entDS, err := chainBadgerDs(entExpPath)
	if err != nil {
		return nil, err
	}

	return &EntBlockstore{
		read:  blockstore.NewBlockstore(lotusDS),
		write: blockstore.NewBlockstore(entDS),
	}, nil
}

func (rb *EntBlockstore) DeleteBlock(c cid.Cid) error {
	return xerrors.Errorf("buffered block store can't delete blocks")
}

func (rb *EntBlockstore) Has(c cid.Cid) (bool, error) {
	if has, err := rb.read.Has(c); err != nil {
		return false, err
	} else if has {
		return true, nil
	}
	return rb.write.Has(c)
}

func (rb *EntBlockstore) Get(c cid.Cid) (blocks.Block, error) {
	if b, err := rb.read.Get(c); err == nil {
		return b, nil
	} else if err != blockstore.ErrNotFound {
		return nil, err
	}
	return rb.write.Get(c)
}

func (rb *EntBlockstore) GetSize(c cid.Cid) (int, error) {
	if s, err := rb.read.GetSize(c); err == nil {
		return s, nil
	} else if err != blockstore.ErrNotFound {
		return 0, err
	}
	return rb.write.GetSize(c)
}

func (rb *EntBlockstore) Put(b blocks.Block) error {
	return rb.write.Put(b)
}

func (rb *EntBlockstore) PutMany(bs []blocks.Block) error {
	return rb.write.PutMany(bs)
}

func (rb *EntBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	// shouldn't call this
	return nil, xerrors.Errorf("redirect block store doesn't support operation")
}

func (rb *EntBlockstore) HashOnRead(enabled bool) {
	rb.read.HashOnRead(enabled)
	rb.write.HashOnRead(enabled)
}
