package lib

import (
	"context"
	"math"

	"github.com/filecoin-project/go-amt-ipld/v2"
	"github.com/filecoin-project/go-hamt-ipld/v2"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	init2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/verifreg"
	states2 "github.com/filecoin-project/specs-actors/v2/actors/states"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"

	"golang.org/x/xerrors"
)

func PrintIpldStats(ctx context.Context, store cbornode.IpldStore, tree *states2.Tree) error {
	var hamtSummaries []*SummaryHAMT
	// Init
	initActor, found, err := tree.GetActor(builtin.InitActorAddr)
	if !found {
		return xerrors.Errorf("init actor not found")
	}
	if err != nil {
		return err
	}
	var initState init2.State
	if err := store.Get(ctx, initActor.Head, &initState); err != nil {
		return err
	}
	if summary, err := measureHAMT(ctx, store, initState.AddressMap, "init.AddressMap"); err != nil {
		return err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}

	// VerifReg
	verifRegActor, found, err := tree.GetActor(builtin.VerifiedRegistryActorAddr)
	if !found {
		return xerrors.Errorf("verified registry actor not found")
	}
	if err != nil {
		return err
	}
	var verifRegState verifreg.State
	if err := store.Get(ctx, verifRegActor.Head, &verifRegState); err != nil {
		return err
	}
	if summary, err := measureHAMT(ctx, store, verifRegState.Verifiers, "verifreg.Verifiers"); err != nil {
		return err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}
	if summary, err := measureHAMT(ctx, store, verifRegState.VerifiedClients, "verifreg.VerifiedClients"); err != nil {
		return err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}

	// Market
	marketActor, found, err := tree.GetActor(builtin.StorageMarketActorAddr)
	if !found {
		return xerrors.Errorf("market actor not found")
	}
	if err != nil {
		return err
	}
	var marketState market.State
	if err := store.Get(ctx, marketActor.Head, &marketState); err != nil {
		return err
	}
	if summary, err := measureHAMT(ctx, store, marketState.PendingProposals, "market.PendingProposals"); err != nil {
		return err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}
	if summary, err := measureHAMT(ctx, store, marketState.EscrowTable, "market.EscrowTable"); err != nil {
		return err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}
	if summary, err := measureHAMT(ctx, store, marketState.LockedTable, "market.LockedTable"); err != nil {
		return err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}
	if summary, err := measureHAMT(ctx, store, marketState.DealOpsByEpoch, "market.DealOpsByEpoch"); err != nil {
		return err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}

	// Power
	powerActor, found, err := tree.GetActor(builtin.StoragePowerActorAddr)
	if !found {
		return xerrors.Errorf("power actor not found")
	}
	if err != nil {
		return err
	}
	var powerState power.State
	err = store.Get(ctx, powerActor.Head, &powerState)
	if err != nil {
		return err
	}
	if summary, err := measureHAMT(ctx, store, powerState.CronEventQueue, "power.CronEventQueue"); err != nil {
		return err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}
	if summary, err := measureHAMT(ctx, store, powerState.Claims, "power.Claims"); err != nil {
		return err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}

	return nil
}

func powerStats(ctx context.Context, store cbornode.IpldStore, tree *states2.Tree) ([]*SummaryHAMT, []*SummaryAggregateHAMT, []*SummaryAMT, []*SummaryAggregateAMT, error) {
	var hamtSummaries []*SummaryHAMT
	powerActor, found, err := tree.GetActor(builtin.StoragePowerActorAddr)
	if !found {
		return nil, nil, nil, nil, xerrors.Errorf("power actor not found")
	}
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var powerState power.State
	err = store.Get(ctx, powerActor.Head, &powerState)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if summary, err := measureHAMT(ctx, store, powerState.CronEventQueue, "power.CronEventQueue"); err != nil {
		return nil, nil, nil, nil, err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}
	if summary, err := measureHAMT(ctx, store, powerState.Claims, "power.Claims"); err != nil {
		return nil, nil, nil, nil, err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}

	return hamtSummaries, nil, nil, nil, nil
}

type SummaryAMT struct {
	ID              string
	AverageDataSize float64
	KeyRange        uint64
	Total           int
}

func measureAMT(ctx context.Context, store cbornode.IpldStore, root cid.Cid, id string) (*SummaryAMT, error) {
	summary := SummaryAMT{ID: id}

	rootNode, err := amt.LoadAMT(ctx, store, root)
	if err != nil {
		return nil, err
	}
	minKey := uint64(math.MaxUint64)
	maxKey := uint64(0)
	err = rootNode.ForEach(ctx, func(k uint64, d *cbg.Deferred) error {
		summary.Total++
		summary.AverageDataSize += float64(len(d.Raw))
		if k < minKey {
			minKey = k
		}
		if k > maxKey {
			maxKey = k
		}
		return nil
	})
	if summary.Total > 0 {
		summary.AverageDataSize = summary.AverageDataSize / float64(summary.Total)
	}
	summary.KeyRange = maxKey - minKey + 1
	return &summary, nil
}

type SummaryAggregateAMT struct {
	ID                       string
	AverageDataSize          float64
	MinAverageDataSizeForAMT float64
	MaxAverageDataSizeForAMT float64

	AverageKeyRange   float64
	MinKeyRangeForAMT uint64
	MaxKeyRangeForAMT uint64

	AverageTotal   float64
	MinTotalForAMT int
	MaxTotalForAMT int
}

func aggregateAMTMeasurements(measures []*SummaryAMT, id string) (*SummaryAggregateAMT, error) {
	dataSizeMin := float64(math.MaxFloat64)
	dataSizeMax := float64(0)
	keyRangeMin := uint64(math.MaxUint64)
	keyRangeMax := uint64(0)
	totalMin := int(math.MaxInt32)
	totalMax := int(0)
	summary := SummaryAggregateAMT{ID: id}

	for _, measure := range measures {
		if measure.ID != id { // only include matches
			return nil, xerrors.Errorf("measure id %s does not match expected %s", measure.ID, id)
		}
		if measure.AverageDataSize < dataSizeMin {
			dataSizeMin = measure.AverageDataSize
		}
		if measure.AverageDataSize > dataSizeMax {
			dataSizeMax = measure.AverageDataSize
		}
		if measure.KeyRange < keyRangeMin {
			keyRangeMin = measure.KeyRange
		}
		if measure.KeyRange > keyRangeMax {
			keyRangeMax = measure.KeyRange
		}
		if measure.Total < totalMin {
			totalMin = measure.Total
		}
		if measure.Total > totalMax {
			totalMax = measure.Total
		}
		summary.AverageDataSize += measure.AverageDataSize
		summary.AverageKeyRange += float64(measure.KeyRange)
		summary.AverageTotal += float64(measure.Total)
	}

	summary.AverageDataSize = summary.AverageDataSize / float64(len(measures))
	summary.MinAverageDataSizeForAMT = dataSizeMin
	summary.MaxAverageDataSizeForAMT = dataSizeMax
	summary.AverageKeyRange = summary.AverageKeyRange / float64(len(measures))
	summary.MinKeyRangeForAMT = keyRangeMin
	summary.MaxKeyRangeForAMT = keyRangeMax
	summary.AverageTotal = summary.AverageTotal / float64(len(measures))
	summary.MinTotalForAMT = totalMin
	summary.MaxTotalForAMT = totalMax
	return &summary, nil
}

type SummaryHAMT struct {
	ID              string
	AverageDataSize float64
	AverageKeySize  float64
	Total           int
}

func measureHAMT(ctx context.Context, store cbornode.IpldStore, root cid.Cid, id string) (*SummaryHAMT, error) {
	summary := SummaryHAMT{ID: id}

	rootNode, err := hamt.LoadNode(ctx, store, root, adt.HamtOptions...)
	if err != nil {
		return nil, err
	}

	err = rootNode.ForEach(ctx, func(k string, val interface{}) error {
		summary.Total++
		// cast value to cbg deferred
		d := val.(*cbg.Deferred)
		summary.AverageDataSize += float64(len(d.Raw))
		summary.AverageKeySize += float64(len([]byte(k)))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if summary.Total > 0 {
		summary.AverageDataSize = summary.AverageDataSize / float64(summary.Total)
		summary.AverageKeySize = summary.AverageKeySize / float64(summary.Total)
	}
	return &summary, nil
}

type SummaryAggregateHAMT struct {
	ID string

	AverageDataSize           float64
	MinAverageDataSizeForHAMT float64
	MaxAverageDataSizeForHAMT float64

	AverageKeySize           float64
	MinAverageKeySizeForHAMT float64
	MaxAverageKeySizeForHAMT float64

	AverageTotal    float64
	MinTotalForHAMT int
	MaxTotalForHAMT int
}

func aggregateHAMTMeasurements(measures []*SummaryHAMT, id string) (*SummaryAggregateHAMT, error) {
	dataSizeMin := float64(math.MaxFloat64)
	dataSizeMax := float64(0)
	keySizeMin := float64(math.MaxFloat64)
	keySizeMax := float64(0)
	totalMin := int(math.MaxInt32)
	totalMax := int(0)
	summary := SummaryAggregateHAMT{ID: id}

	for _, measure := range measures {
		if measure.ID != id { // only include matches
			return nil, xerrors.Errorf("measure id %s does not match expected %s", measure.ID, id)
		}
		if measure.AverageDataSize < dataSizeMin {
			dataSizeMin = measure.AverageDataSize
		}
		if measure.AverageDataSize > dataSizeMax {
			dataSizeMax = measure.AverageDataSize
		}
		if measure.AverageKeySize < keySizeMin {
			keySizeMin = measure.AverageKeySize
		}
		if measure.AverageKeySize > keySizeMax {
			keySizeMax = measure.AverageKeySize
		}
		if measure.Total < totalMin {
			totalMin = measure.Total
		}
		if measure.Total > totalMax {
			totalMax = measure.Total
		}
		summary.AverageDataSize += measure.AverageDataSize
		summary.AverageKeySize += measure.AverageKeySize
		summary.AverageTotal += float64(measure.Total)
	}

	summary.AverageDataSize = summary.AverageDataSize / float64(len(measures))
	summary.MinAverageDataSizeForHAMT = dataSizeMin
	summary.MaxAverageDataSizeForHAMT = dataSizeMax
	summary.AverageKeySize = summary.AverageKeySize / float64(len(measures))
	summary.MinAverageKeySizeForHAMT = keySizeMin
	summary.MaxAverageKeySizeForHAMT = keySizeMax
	summary.AverageTotal = summary.AverageTotal / float64(len(measures))
	summary.MinTotalForHAMT = totalMin
	summary.MaxTotalForHAMT = totalMax
	return &summary, nil
}
