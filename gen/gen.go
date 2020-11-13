package main

import (
	"github.com/filecoin-project/ent/lib"
	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	if err := gen.WriteTupleEncodersToFile("./lib/cbor_gen.go", "lib",
		lib.Ticket{},
		lib.BeaconEntry{},
		lib.ElectionProof{},
		lib.BlockHeader{},
	); err != nil {
		panic(err)
	}
}
