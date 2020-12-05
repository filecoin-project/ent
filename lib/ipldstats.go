package lib

import (
	"context"
	"fmt"
	"math"
	"sort"

	address "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-amt-ipld/v2"
	"github.com/filecoin-project/go-hamt-ipld/v2"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	states2 "github.com/filecoin-project/specs-actors/v2/actors/states"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"

	"golang.org/x/xerrors"
)

func PrintIpldStats(ctx context.Context, store cbornode.IpldStore, tree *states2.Tree, verbose bool) error {
	var hamtSummaries []*SummaryHAMT
	var hamtAggrSummaries []*SummaryAggregateHAMT
	var amtSummaries []*SummaryAMT
	var amtAggrSummaries []*SummaryAggregateAMT

	// Init
	// if verbose {
	// 	fmt.Printf("Init\n")
	// }
	// initActor, found, err := tree.GetActor(builtin.InitActorAddr)
	// if !found {
	// 	return xerrors.Errorf("init actor not found")
	// }
	// if err != nil {
	// 	return err
	// }
	// var initState init2.State
	// if err := store.Get(ctx, initActor.Head, &initState); err != nil {
	// 	return err
	// }
	// if summary, err := measureHAMT(ctx, store, initState.AddressMap, "init.AddressMap"); err != nil {
	// 	return err
	// } else {
	// 	hamtSummaries = append(hamtSummaries, summary)
	// }

	// // Power
	// if verbose {
	// 	fmt.Printf("Power\n")
	// }
	// powerHAMTSummaries, _, _, powerAggrAMTSummaries, err := powerStats(ctx, store, tree)
	// if err != nil {
	// 	return err
	// }
	// hamtSummaries = append(hamtSummaries, powerHAMTSummaries...)
	// amtAggrSummaries = append(amtAggrSummaries, powerAggrAMTSummaries...)

	// // Market
	// if verbose {
	// 	fmt.Printf("Market\n")
	// }

	// marketHAMTSummaries, marketAggrHAMTSummaries, marketAMTSummaries, err := marketStats(ctx, store, tree)
	// if err != nil {
	// 	return err
	// }
	// hamtSummaries = append(hamtSummaries, marketHAMTSummaries...)
	// hamtAggrSummaries = append(hamtAggrSummaries, marketAggrHAMTSummaries...)
	// amtSummaries = append(amtSummaries, marketAMTSummaries...)

	// Miner

	activeAddrs, _, err := activeMiners(ctx, store, tree)
	if err != nil {
		return err
	}
	if verbose {
		fmt.Printf("Miner, %d to go\n", len(activeAddrs))
	}
	minerAggrHAMTSummaries, minerAggrAMTSummaries, err := minerStats(ctx, store, tree, activeAddrs, verbose)
	if err != nil {
		return err
	}
	hamtAggrSummaries = append(hamtAggrSummaries, minerAggrHAMTSummaries...)
	amtAggrSummaries = append(amtAggrSummaries, minerAggrAMTSummaries...)

	// // Print stats
	sort.Slice(hamtSummaries, func(i int, j int) bool {
		return hamtSummaries[i].ID < hamtSummaries[j].ID
	})
	sort.Slice(amtSummaries, func(i int, j int) bool {
		return amtSummaries[i].ID < amtSummaries[j].ID
	})
	sort.Slice(hamtAggrSummaries, func(i int, j int) bool {
		return hamtAggrSummaries[i].ID < hamtAggrSummaries[j].ID
	})
	sort.Slice(amtAggrSummaries, func(i int, j int) bool {
		return amtAggrSummaries[i].ID < amtAggrSummaries[j].ID
	})
	// fmt.Printf("Total Miners: %d, Active Miners: %d\n", totalClaims, len(activeAddrs))
	// fmt.Printf("Singleton AMTs\n")
	// fmt.Printf("ID, AverageDataSize, KeyRange, Total\n")
	// for _, summary := range amtSummaries {
	// 	//		summary.Print()
	// }

	// fmt.Printf("Aggregate AMTs\n")
	// fmt.Printf("ID, AverageDataSize, MinAvgDataSize, MaxAvgDataSize, AverageKeyRange, MinKeyRange, MaxKeyRange, AverageTotal, MinTotal, MaxTotal")
	for _, summary := range amtAggrSummaries {
		//		summary.Print()
		summary.PrintHist()
	}

	// fmt.Printf("Singleton HAMTs\n")
	// fmt.Printf("ID, AverageDataSize, AverageKeySize, Total\n")
	// for _, summary := range hamtSummaries {
	// 	//		summary.Print()
	// }

	// fmt.Printf("Aggregate HAMTs\n")
	// fmt.Printf("ID, AverageDataSize, MinAvgDataSize, MaxAvgDataSize, AverageKeySize, MinAvgKeySize, MaxAvgKeySize, AverageTotal, MinTotal, MaxTotal\n")
	for _, summary := range hamtAggrSummaries {
		//		summary.Print()
		summary.PrintHist()
	}

	return nil
}

func minerStats(ctx context.Context, store cbornode.IpldStore, tree *states2.Tree, active []address.Address, verbose bool) ([]*SummaryAggregateHAMT, []*SummaryAggregateAMT, error) {
	var precommitSectors []*SummaryHAMT
	var precommitSectorExpiry []*SummaryAMT
	var sectors []*SummaryAMT
	var deadlinePartitions []*SummaryAMT
	var deadlineExpirationEpochs []*SummaryAMT
	var partitionExpirationEpochs []*SummaryAMT
	var partitionEarlyTerminated []*SummaryAMT

	var aggrHAMTSummaries []*SummaryAggregateHAMT
	var aggrAMTSummaries []*SummaryAggregateAMT

	for i, a := range active {
		if verbose && i%50 == 0 {
			fmt.Printf("%d miners processed\n", i)
		}
		minerActor, found, err := tree.GetActor(a)
		if !found {
			return nil, nil, xerrors.Errorf("miner actor with non zero claim %s not found", a)
		}
		if err != nil {
			return nil, nil, err
		}
		var minerState miner.State
		if err := store.Get(ctx, minerActor.Head, &minerState); err != nil {
			return nil, nil, err
		}
		if summary, err := measureHAMT(ctx, store, minerState.PreCommittedSectors, "miner.PreCommittedSectors"); err != nil {
			return nil, nil, err
		} else {
			precommitSectors = append(precommitSectors, summary)
		}
		if summary, err := measureAMT(ctx, store, minerState.PreCommittedSectorsExpiry, "miner.PreCommittedSectorsExpiry"); err != nil {
			return nil, nil, err
		} else {
			precommitSectorExpiry = append(precommitSectorExpiry, summary)
		}
		if summary, err := measureAMT(ctx, store, minerState.Sectors, "miner.Sectors"); err != nil {
			return nil, nil, err
		} else {
			sectors = append(sectors, summary)
		}
		dls, err := minerState.LoadDeadlines(adt.WrapStore(ctx, store))
		if err != nil {
			return nil, nil, err
		}
		err = dls.ForEach(adt.WrapStore(ctx, store), func(dlIdx uint64, dl *miner.Deadline) error {
			if dl.TotalSectors == 0 {
				return nil // skip empty deadlines
			}
			if summary, err := measureAMT(ctx, store, dl.Partitions, "miner.DeadlinePartitions"); err != nil {
				return err
			} else {
				deadlinePartitions = append(deadlinePartitions, summary)
			}
			if summary, err := measureAMT(ctx, store, dl.ExpirationsEpochs, "miner.DeadlineExpirationEpochs"); err != nil {
				return err
			} else {
				deadlineExpirationEpochs = append(deadlineExpirationEpochs, summary)
			}
			parts, err := dl.PartitionsArray(adt.WrapStore(ctx, store))
			if err != nil {
				return err
			}
			var partition miner.Partition
			err = parts.ForEach(&partition, func(i int64) error {
				if summary, err := measureAMT(ctx, store, partition.ExpirationsEpochs, "miner.PartitionExpirationEpochs"); err != nil {
					return err
				} else {
					partitionExpirationEpochs = append(partitionExpirationEpochs, summary)
				}
				if summary, err := measureAMT(ctx, store, partition.EarlyTerminated, "miner.PartitionEarlyTerminated"); err != nil {
					return err
				} else {
					partitionEarlyTerminated = append(partitionEarlyTerminated, summary)
				}
				return nil
			})
			return err
		})
		if err != nil {
			return nil, nil, err
		}
	}

	// aggregate stats
	if summary, err := aggregateHAMTMeasurements(precommitSectors, "miner.PreCommittedSectors"); err != nil {
		return nil, nil, err
	} else {
		aggrHAMTSummaries = append(aggrHAMTSummaries, summary)
	}
	if summary, err := aggregateAMTMeasurements(precommitSectorExpiry, "miner.PreCommittedSectorsExpiry"); err != nil {
		return nil, nil, err
	} else {
		aggrAMTSummaries = append(aggrAMTSummaries, summary)
	}
	if summary, err := aggregateAMTMeasurements(sectors, "miner.Sectors"); err != nil {
		return nil, nil, err
	} else {
		aggrAMTSummaries = append(aggrAMTSummaries, summary)
	}
	if summary, err := aggregateAMTMeasurements(deadlinePartitions, "miner.DeadlinePartitions"); err != nil {
		return nil, nil, err
	} else {
		aggrAMTSummaries = append(aggrAMTSummaries, summary)
	}
	if summary, err := aggregateAMTMeasurements(deadlineExpirationEpochs, "miner.DeadlineExpirationEpochs"); err != nil {
		return nil, nil, err
	} else {
		aggrAMTSummaries = append(aggrAMTSummaries, summary)
	}
	if summary, err := aggregateAMTMeasurements(partitionExpirationEpochs, "miner.PartitionExpirationEpochs"); err != nil {
		return nil, nil, err
	} else {
		aggrAMTSummaries = append(aggrAMTSummaries, summary)
	}
	if summary, err := aggregateAMTMeasurements(partitionEarlyTerminated, "miner.PartitionEarlyTerminated"); err != nil {
		return nil, nil, err
	} else {
		aggrAMTSummaries = append(aggrAMTSummaries, summary)
	}
	return aggrHAMTSummaries, aggrAMTSummaries, nil
}

func marketStats(ctx context.Context, store cbornode.IpldStore, tree *states2.Tree) ([]*SummaryHAMT, []*SummaryAggregateHAMT, []*SummaryAMT, error) {
	var hamtSummaries []*SummaryHAMT
	var amtSummaries []*SummaryAMT
	var aggrHAMTSummaries []*SummaryAggregateHAMT

	marketActor, found, err := tree.GetActor(builtin.StorageMarketActorAddr)
	if !found {
		return nil, nil, nil, xerrors.Errorf("market actor not found")
	}
	if err != nil {
		return nil, nil, nil, err
	}
	var marketState market.State
	if err := store.Get(ctx, marketActor.Head, &marketState); err != nil {
		return nil, nil, nil, err
	}
	// Singleton HAMTs
	if summary, err := measureHAMT(ctx, store, marketState.PendingProposals, "market.PendingProposals"); err != nil {
		return nil, nil, nil, err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}
	if summary, err := measureHAMT(ctx, store, marketState.EscrowTable, "market.EscrowTable"); err != nil {
		return nil, nil, nil, err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}
	if summary, err := measureHAMT(ctx, store, marketState.LockedTable, "market.LockedTable"); err != nil {
		return nil, nil, nil, err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}
	if summary, err := measureHAMT(ctx, store, marketState.DealOpsByEpoch, "market.DealOpsByEpoch"); err != nil {
		return nil, nil, nil, err
	} else {
		hamtSummaries = append(hamtSummaries, summary)
	}

	// Singleton AMTs
	if summary, err := measureAMT(ctx, store, marketState.Proposals, "market.Proposals"); err != nil {
		return nil, nil, nil, err
	} else {
		amtSummaries = append(amtSummaries, summary)
	}
	if summary, err := measureAMT(ctx, store, marketState.States, "market.States"); err != nil {
		return nil, nil, nil, err
	} else {
		amtSummaries = append(amtSummaries, summary)
	}

	// Aggregate HAMT
	dobe, err := adt.AsMap(adt.WrapStore(ctx, store), marketState.DealOpsByEpoch)
	if err != nil {
		return nil, nil, nil, err
	}
	var dealSet cbg.CborCid
	var dealOpsByEpochLayer2Measures []*SummaryHAMT
	err = dobe.ForEach(&dealSet, func(a string) error {
		measure, err := measureHAMT(ctx, store, cid.Cid(dealSet), "market.DealOpsByEpochLayer2")
		if err != nil {
			return err
		}
		dealOpsByEpochLayer2Measures = append(dealOpsByEpochLayer2Measures, measure)
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if summary, err := aggregateHAMTMeasurements(dealOpsByEpochLayer2Measures, "market.DealOpsByEpochLayer2"); err != nil {
		return nil, nil, nil, err
	} else {
		aggrHAMTSummaries = append(aggrHAMTSummaries, summary)
	}

	return hamtSummaries, aggrHAMTSummaries, amtSummaries, nil
}

func powerStats(ctx context.Context, store cbornode.IpldStore, tree *states2.Tree) ([]*SummaryHAMT, []*SummaryAggregateHAMT, []*SummaryAMT, []*SummaryAggregateAMT, error) {
	var hamtSummaries []*SummaryHAMT
	var aggrAMTSummaries []*SummaryAggregateAMT
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
	// Singleton HAMTs
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

	// Aggregate AMTs
	cronQueue, err := adt.AsMap(adt.WrapStore(ctx, store), powerState.CronEventQueue)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var cronEvents cbg.CborCid
	var cronEventQueueLayer2Measures []*SummaryAMT
	err = cronQueue.ForEach(&cronEvents, func(a string) error {
		measure, err := measureAMT(ctx, store, cid.Cid(cronEvents), "power.CronEventQueueLayer2")
		if err != nil {
			return err
		}
		cronEventQueueLayer2Measures = append(cronEventQueueLayer2Measures, measure)
		return nil
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if summary, err := aggregateAMTMeasurements(cronEventQueueLayer2Measures, "power.CronEventQueueLayer2"); err != nil {
		return nil, nil, nil, nil, err
	} else {
		aggrAMTSummaries = append(aggrAMTSummaries, summary)
	}

	return hamtSummaries, nil, nil, aggrAMTSummaries, nil
}

func activeMiners(ctx context.Context, store cbornode.IpldStore, tree *states2.Tree) ([]address.Address, int, error) {
	powerActor, found, err := tree.GetActor(builtin.StoragePowerActorAddr)
	if !found {
		return nil, 0, xerrors.Errorf("power actor not found")
	}
	if err != nil {
		return nil, 0, err
	}
	var powerState power.State
	err = store.Get(ctx, powerActor.Head, &powerState)
	if err != nil {
		return nil, 0, err
	}
	// Walk claims and record all addresses with more than 0 power
	claimsTable, err := adt.AsMap(adt.WrapStore(ctx, store), powerState.Claims)
	if err != nil {
		return nil, 0, err
	}
	var activeAddrs []address.Address
	var claim power.Claim
	total := 0
	err = claimsTable.ForEach(&claim, func(a string) error {
		total++
		if claim.RawBytePower.GreaterThan(big.Zero()) {
			addr, err := address.NewFromBytes([]byte(a))
			if err != nil {
				return err
			}
			activeAddrs = append(activeAddrs, addr)
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return activeAddrs, total, nil
}

type SummaryAMT struct {
	ID              string
	AverageDataSize float64
	KeyRange        uint64
	Total           int
}

func (s *SummaryAMT) Print() {
	fmt.Printf("%s, %f, %d, %d\n", s.ID, s.AverageDataSize, s.KeyRange, s.Total)
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

	SizeHist     *Histogram
	DataSizeHist *Histogram
	KeyRangeHist *Histogram
}

func (s *SummaryAggregateAMT) Print() {
	fmt.Printf(
		"%s, %f, %f, %f, %f, %d, %d, %f, %d, %d",
		s.ID, s.AverageDataSize, s.MinAverageDataSizeForAMT, s.MaxAverageDataSizeForAMT,
		s.AverageKeyRange, s.MinKeyRangeForAMT, s.MaxKeyRangeForAMT,
		s.AverageTotal, s.MinTotalForAMT, s.MaxTotalForAMT,
	)
}

func (s *SummaryAggregateAMT) PrintHist() {
	if s.SizeHist != nil && s.DataSizeHist != nil && s.KeyRangeHist != nil {
		fmt.Printf("Hist for %s\n", s.ID)
		fmt.Printf("Totals\n")
		fmt.Printf("Zero Count: %d\n", s.SizeHist.zeroCnt)
		s.SizeHist.print()
		fmt.Printf("DataSize\n")
		s.DataSizeHist.print()
		fmt.Printf("KeyRange\n")
		s.KeyRangeHist.print()
		fmt.Printf("--------------------------------------------------------------------------------\n")
	}
}

func aggregateAMTMeasurements(measures []*SummaryAMT, id string) (*SummaryAggregateAMT, error) {
	dataSizeMin := float64(math.MaxFloat64)
	dataSizeMax := float64(0)
	keyRangeMin := uint64(math.MaxUint64)
	keyRangeMax := uint64(0)
	totalMin := int(math.MaxInt32)
	totalMax := int(0)
	summary := SummaryAggregateAMT{ID: id}

	_, doHists := amtHistSizes[id]
	if doHists {
		summary.SizeHist = &Histogram{
			binSize: amtHistSizes[id].sizeBinSize,
			bins:    make(map[int64]int64),
		}
		summary.DataSizeHist = &Histogram{
			binSize: amtHistSizes[id].dataSizeBinSize,
			bins:    make(map[int64]int64),
		}
		summary.KeyRangeHist = &Histogram{
			binSize: amtHistSizes[id].keyRangeBinSize,
			bins:    make(map[int64]int64),
		}
	}

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

		if doHists {
			summary.SizeHist.add(float64(measure.Total), int64(1))
			summary.DataSizeHist.add(measure.AverageDataSize, int64(1))
			summary.KeyRangeHist.add(float64(measure.KeyRange), int64(1))
		}
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

func (s *SummaryHAMT) Print() {
	fmt.Printf("%s, %f, %f, %d\n", s.ID, s.AverageDataSize, s.AverageKeySize, s.Total)
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

	SizeHist     *Histogram
	DataSizeHist *Histogram
	KeySizeHist  *Histogram
}

func (s *SummaryAggregateHAMT) Print() {
	fmt.Printf(
		"%s, %f, %f, %f, %f, %f, %f, %f, %d, %d\n",
		s.ID, s.AverageDataSize, s.MinAverageDataSizeForHAMT, s.MaxAverageDataSizeForHAMT,
		s.AverageKeySize, s.MinAverageKeySizeForHAMT, s.MaxAverageKeySizeForHAMT,
		s.AverageTotal, s.MinTotalForHAMT, s.MaxTotalForHAMT,
	)
}

func (s *SummaryAggregateHAMT) PrintHist() {
	if s.SizeHist != nil && s.DataSizeHist != nil && s.KeySizeHist != nil {
		fmt.Printf("Hist for %s\n", s.ID)
		fmt.Printf("Totals\n")
		fmt.Printf("Zero Count: %d\n", s.SizeHist.zeroCnt)
		s.SizeHist.print()
		fmt.Printf("DataSize\n")
		s.DataSizeHist.print()
		fmt.Printf("KeySize\n")
		s.KeySizeHist.print()
	}
}

func aggregateHAMTMeasurements(measures []*SummaryHAMT, id string) (*SummaryAggregateHAMT, error) {
	dataSizeMin := float64(math.MaxFloat64)
	dataSizeMax := float64(0)
	keySizeMin := float64(math.MaxFloat64)
	keySizeMax := float64(0)
	totalMin := int(math.MaxInt32)
	totalMax := int(0)
	summary := SummaryAggregateHAMT{ID: id}

	doHists := id == "miner.PreCommittedSectors"

	if doHists {
		summary.SizeHist = &Histogram{
			binSize: 50,
			bins:    make(map[int64]int64),
		}
		summary.DataSizeHist = &Histogram{
			binSize: 50,
			bins:    make(map[int64]int64),
		}
		summary.KeySizeHist = &Histogram{
			binSize: 1,
			bins:    make(map[int64]int64),
		}
	}

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
		if doHists {
			summary.SizeHist.add(float64(measure.Total), int64(1))
			summary.DataSizeHist.add(measure.AverageDataSize, int64(1))
			summary.KeySizeHist.add(float64(measure.AverageKeySize), int64(1))
		}
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

type histSizes struct {
	sizeBinSize     int64
	dataSizeBinSize int64
	keyRangeBinSize int64
}

var amtHistSizes = map[string]histSizes{
	"miner.Sectors": histSizes{
		sizeBinSize:     1000,
		dataSizeBinSize: 50,
		keyRangeBinSize: 10000,
	},
	"miner.DeadlinePartitions": histSizes{
		sizeBinSize:     1,
		dataSizeBinSize: 100,
		keyRangeBinSize: 1,
	},
	"miner.PartitionExpirationEpochs": histSizes{
		dataSizeBinSize: 100,
		keyRangeBinSize: 100000,
		sizeBinSize:     5,
	},
	"miner.DeadlineExpirationEpochs": histSizes{
		dataSizeBinSize: 1,
		keyRangeBinSize: 100000,
		sizeBinSize:     5,
	},
	"miner.PreCommittedSectorsExpiry": histSizes{
		dataSizeBinSize: 20,
		keyRangeBinSize: 100,
		sizeBinSize:     10,
	},
}

type Histogram struct {
	binSize int64
	bins    map[int64]int64
	zeroCnt int64
}

func (h *Histogram) add(vf float64, f int64) {
	v := int64(vf)
	if _, ok := h.bins[v/h.binSize]; !ok {
		h.bins[v/h.binSize] = 0
	}
	h.bins[v/h.binSize] += f
	if vf == float64(0) {
		h.zeroCnt++
	}
}

func (h *Histogram) print() {
	keys := make([]int64, len(h.bins))
	i := 0
	for key := range h.bins {
		keys[i] = key
		i++
	}
	sort.Slice(keys, func(i int, j int) bool {
		return keys[i] < keys[j]
	})
	fmt.Printf("bin start,frequency\n")
	for _, key := range keys {
		binStart := key * h.binSize // start value point of bin
		fmt.Printf("%d, %d\n", binStart, h.bins[key])
	}

}
