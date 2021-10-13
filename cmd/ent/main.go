package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	adt0 "github.com/filecoin-project/specs-actors/actors/util/adt"
	builtin2 "github.com/filecoin-project/specs-actors/v2/actors/builtin"
	migration4 "github.com/filecoin-project/specs-actors/v2/actors/migration/nv4"
	migration7 "github.com/filecoin-project/specs-actors/v2/actors/migration/nv7"
	states2 "github.com/filecoin-project/specs-actors/v2/actors/states"
	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	migration10 "github.com/filecoin-project/specs-actors/v3/actors/migration/nv10"
	states3 "github.com/filecoin-project/specs-actors/v3/actors/states"
	adt3 "github.com/filecoin-project/specs-actors/v3/actors/util/adt"
	builtin4 "github.com/filecoin-project/specs-actors/v4/actors/builtin"
	migration12 "github.com/filecoin-project/specs-actors/v4/actors/migration/nv12"
	states4 "github.com/filecoin-project/specs-actors/v4/actors/states"
	adt4 "github.com/filecoin-project/specs-actors/v4/actors/util/adt"
	builtin5 "github.com/filecoin-project/specs-actors/v5/actors/builtin"
	migration13 "github.com/filecoin-project/specs-actors/v5/actors/migration/nv13"
	states5 "github.com/filecoin-project/specs-actors/v5/actors/states"
	adt5 "github.com/filecoin-project/specs-actors/v5/actors/util/adt"
	builtin6 "github.com/filecoin-project/specs-actors/v6/actors/builtin"
	migration14 "github.com/filecoin-project/specs-actors/v6/actors/migration/nv14"
	states6 "github.com/filecoin-project/specs-actors/v6/actors/states"

	cid "github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/ent/lib"
)

var migrateCmd = &cli.Command{
	Name:        "migrate",
	Description: "migrate a filecoin state root",
	Subcommands: []*cli.Command{
		{
			Name:   "v6",
			Usage:  "migrate a filecoin state tree from v5 to v6",
			Action: runMigrateV5ToV6Cmd,
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "validate"},
				&cli.StringFlag{Name: "read-cache"},
				&cli.StringFlag{Name: "write-cache"},
			},
		},

		{
			Name:   "v5",
			Usage:  "migrate a filecoin state tree from v4 to v5",
			Action: runMigrateV4ToV5Cmd,
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "validate"},
				&cli.StringFlag{Name: "read-cache"},
				&cli.BoolFlag{Name: "write-cache"},
			},
		},

		{
			Name:   "v4",
			Usage:  "migrate a filecoin state tree from v3 to v4",
			Action: runMigrateV3ToV4Cmd,
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "validate"},
				&cli.StringFlag{Name: "read-cache"},
				&cli.BoolFlag{Name: "write-cache"},
			},
		},

		{
			Name:   "v3",
			Usage:  "migrate a single state tree from v2 to v3",
			Action: runMigrateV2ToV3Cmd,
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "validate"},
				&cli.StringFlag{Name: "read-cache"},
				&cli.BoolFlag{Name: "write-cache"},
			},
		},
		{
			Name:   "v2",
			Usage:  "migrate a single state tree from v1 to v2",
			Action: runMigrateV1ToV2Cmd,
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "validate"},
			},
		},
	},
}

var validateCmd = &cli.Command{
	Name:        "validate",
	Description: "validate a statetree by checking lots of invariants",
	Subcommands: []*cli.Command{
		{
			Name:   "v6",
			Usage:  "validate a v6 state tree",
			Action: runValidateV6Cmd,
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "unwrapped"},
			},
		},
		{
			Name:   "v5",
			Usage:  "validate a v5 state tree",
			Action: runValidateV5Cmd,
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "unwrapped"},
			},
		},
		{
			Name:   "v4",
			Usage:  "validate a v4 state tree",
			Action: runValidateV4Cmd,
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "unwrapped"},
			},
		},
		{
			Name:   "v3",
			Usage:  "validation a single v3 state tree",
			Action: runValidateV3Cmd,
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "unwrapped"},
			},
		},
		{
			Name:   "v2",
			Usage:  "validate a single v2 state tree",
			Action: runValidateV2Cmd,
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "unwrapped"},
			},
		},
	},
}

var infoCmd = &cli.Command{
	Name:        "info",
	Description: "report blockchain and state info on latest state version",
	Subcommands: []*cli.Command{
		{
			Name:        "roots",
			Description: "provide state tree root cids for migrating",
			Action:      runRootsCmd,
		},
		{
			Name:        "debts",
			Description: "display all miner actors in debt and total burnt funds",
			Action:      runDebtsCmd,
		},
		{
			Name:        "balances",
			Description: "display all miner actor locked funds and available balances",
			Action:      runBalancesCmd,
		},
		{
			Name:        "export-sectors",
			Description: "exports all on-chain sectors",
			Action:      runExportSectorsCmd,
		},
	},
}

type ActorsVersion int

const (
	V2 = iota + 2
	V3
	V4
	V5
	V6
)

var migrateFuncs = map[ActorsVersion]func(context.Context, cid.Cid, string, cbornode.IpldStore, abi.ChainEpoch, *lib.MigrationLogger) (cid.Cid, time.Duration, func() error, error){
	V2: migrateV1ToV2,
	V3: migrateV2ToV3,
	V4: migrateV3ToV4,
	V5: migrateV4ToV5,
	V6: migrateV5ToV6,
}

var validateFuncs = map[ActorsVersion]func(context.Context, cbornode.IpldStore, abi.ChainEpoch, cid.Cid, bool) error{
	V2: validateV2,
	V3: validateV3,
	V4: validateV4,
	V5: validateV5,
	V6: validateV6,
}

func main() {
	// pprof server
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	app := &cli.App{
		Name:        "ent",
		Usage:       "Test filecoin state tree migrations by running them",
		Description: "Test filecoin state tree migrations by running them",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "cpuprofile",
				Usage: "run cpuprofile and write results to provided file path",
			},
		},
		Commands: []*cli.Command{
			migrateCmd,
			validateCmd,
			infoCmd,
		},
	}
	sort.Sort(cli.CommandsByName(app.Commands))
	for _, c := range app.Commands {
		sort.Sort(cli.FlagsByName(c.Flags))
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func runMigrateCmd(c *cli.Context, v ActorsVersion) error {
	if c.Args().Len() != 2 {
		return xerrors.Errorf("not enough args, need state root to migrate and height of state")
	}

	if c.Args().Len() != 2 {
		return xerrors.Errorf("not enough args, need state root to migrate and height of state")
	}
	cleanUp, err := cpuProfile(c)
	if err != nil {
		return err
	}
	defer cleanUp()

	log := lib.NewMigrationLogger(os.Stdout)

	stateRootInRaw, err := cid.Decode(c.Args().First())
	if err != nil {
		return err
	}
	hRaw, err := strconv.Atoi(c.Args().Get(1))
	if err != nil {
		return err
	}
	height := abi.ChainEpoch(int64(hRaw))
	chn := lib.Chain{}

	// Migrate State
	store, err := chn.LoadCborStore(c.Context)
	if err != nil {
		return err
	}
	stateRootIn, err := loadStateRoot(c.Context, store, stateRootInRaw)
	if err != nil {
		return err
	}
	m, ok := migrateFuncs[v]
	if !ok {
		return fmt.Errorf("unsupported actors version %d for migration\n", v)
	}
	stateRootOut, duration, cacheWriteCB, err := m(c.Context, stateRootIn, c.String("read-cache"), store, height, log)
	if err != nil {
		return err
	}
	fmt.Printf("%s => %s -- %v\n", stateRootIn, stateRootOut, duration)

	// Measure flush time
	writeStart := time.Now()
	if err := chn.FlushBufferedState(c.Context, stateRootOut); err != nil {
		return xerrors.Errorf("failed to flush state tree to disk: %w\n", err)
	}
	writeDuration := time.Since(writeStart)
	fmt.Printf("%s buffer flush time: %v\n", stateRootOut, writeDuration)

	if c.Bool("write-cache") {
		if err := cacheWriteCB(); err != nil {
			return err
		}
	}

	if c.Bool("validate") {
		val, ok := validateFuncs[v]
		if !ok {
			return fmt.Errorf("unsupported actors version %d for validation\n", v)
		}

		err := val(c.Context, store, height, stateRootOut, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func runMigrateV5ToV6Cmd(c *cli.Context) error {
	return runMigrateCmd(c, V6)
}

func runMigrateV4ToV5Cmd(c *cli.Context) error {
	return runMigrateCmd(c, V5)
}

func runMigrateV3ToV4Cmd(c *cli.Context) error {
	return runMigrateCmd(c, V4)
}

func runMigrateV2ToV3Cmd(c *cli.Context) error {
	return runMigrateCmd(c, V3)
}

func runMigrateV1ToV2Cmd(c *cli.Context) error {
	return runMigrateCmd(c, V2)
}

func runValidateCmd(c *cli.Context, v ActorsVersion) error {
	if c.Args().Len() != 2 {
		return xerrors.Errorf("wrong number of args, need state root to migrate and height")
	}
	cleanUp, err := cpuProfile(c)
	if err != nil {
		return err
	}
	defer cleanUp()

	stateRoot, err := cid.Decode(c.Args().First())
	if err != nil {
		return err
	}
	hRaw, err := strconv.Atoi(c.Args().Get(1))
	if err != nil {
		return err
	}
	height := abi.ChainEpoch(int64(hRaw))
	chn := lib.Chain{}
	store, err := chn.LoadCborStore(c.Context)
	if err != nil {
		return err
	}
	wrapped := true
	if c.Bool("unwrapped") {
		wrapped = false
	}
	val, ok := validateFuncs[v]
	if !ok {
		return fmt.Errorf("unsupported actors version %d for validation\n", v)
	}
	return val(c.Context, store, height, stateRoot, wrapped)
}

func runValidateV6Cmd(c *cli.Context) error {
	return runValidateCmd(c, V6)
}

func runValidateV5Cmd(c *cli.Context) error {
	return runValidateCmd(c, V5)
}

func runValidateV4Cmd(c *cli.Context) error {
	return runValidateCmd(c, V4)
}

func runValidateV3Cmd(c *cli.Context) error {
	return runValidateCmd(c, V3)
}

func runValidateV2Cmd(c *cli.Context) error {
	return runValidateCmd(c, V2)
}

func runRootsCmd(c *cli.Context) error {
	if c.Args().Len() < 2 {
		return xerrors.Errorf("not enough args, need chain tip and number of states to fetch")
	}

	bcid, err := cid.Decode(c.Args().First())
	if err != nil {
		return err
	}
	num, err := strconv.Atoi(c.Args().Get(1))
	if err != nil {
		return err
	}
	// Read roots and epoch of creation from lotus datastore
	roots := make([]lib.IterVal, num)
	chn := lib.Chain{}
	iter, err := chn.NewChainStateIterator(c.Context, bcid)
	if err != nil {
		return err
	}
	for i := 0; !iter.Done() && i < num; i++ {
		roots[i] = iter.Val()
		if err := iter.Step(c.Context); err != nil {
			return err
		}
	}
	// Output roots
	for _, val := range roots {
		fmt.Printf("Epoch %d: %s \n", val.Height, val.State)
	}
	return nil
}

func runDebtsCmd(c *cli.Context) error {
	if !c.Args().Present() {
		return xerrors.Errorf("not enough args, need state root")
	}
	stateRootIn, err := cid.Decode(c.Args().First())
	if err != nil {
		return err
	}
	chn := lib.Chain{}
	store, err := chn.LoadCborStore(c.Context)
	if err != nil {
		return err
	}

	bf, err := migration4.InputTreeBurntFunds(c.Context, store, stateRootIn)
	if err != nil {
		return err
	}

	available, err := migration4.InputTreeMinerAvailableBalance(c.Context, store, stateRootIn)
	if err != nil {
		return err
	}
	// filter out positive balances
	totalDebt := big.Zero()
	for addr, balance := range available {
		if balance.LessThan(big.Zero()) {
			debt := balance.Neg()
			fmt.Printf("miner %s: %s\n", addr, debt)
			totalDebt = big.Add(totalDebt, debt)
		}
	}
	fmt.Printf("burnt funds balance: %s\n", bf)
	fmt.Printf("total debt:          %s\n", totalDebt)
	return nil
}

func runBalancesCmd(c *cli.Context) error {
	if !c.Args().Present() {
		return xerrors.Errorf("not enough args, need state root")
	}
	stateRootIn, err := cid.Decode(c.Args().First())
	if err != nil {
		return err
	}
	chn := lib.Chain{}
	store, err := chn.LoadCborStore(c.Context)
	if err != nil {
		return err
	}

	balances, err := lib.V0TreeMinerBalances(c.Context, store, stateRootIn)
	if err != nil {
		return err
	}
	// Print miner address, locked balance, and available balance (balance - lb - pcd - ip)
	for addr, bi := range balances {
		minerLiabilities := big.Sum(bi.LockedFunds, bi.PreCommitDeposits, bi.InitialPledge)
		availableBalance := big.Sub(bi.Balance, minerLiabilities)
		fmt.Printf("%s,%v,%v\n", addr, bi.LockedFunds, availableBalance)
	}
	return nil
}

func runExportSectorsCmd(c *cli.Context) error {
	if !c.Args().Present() {
		return xerrors.Errorf("not enough args, need state root")
	}
	stateRootIn, err := cid.Decode(c.Args().First())
	if err != nil {
		return err
	}
	chn := lib.Chain{}
	store, err := chn.LoadCborStore(c.Context)
	if err != nil {
		return err
	}

	tree, err := loadStateTreeV2(c.Context, store, stateRootIn)
	if err != nil {
		return err
	}

	sectors, err := lib.ExportSectors(c.Context, adt0.WrapStore(c.Context, store), tree)
	if err != nil {
		return err
	}

	// Print JSON representation of sector infos, one per line.
	keepGoing := true
	for keepGoing {
		sinfo, ok := <-sectors
		j, err := json.Marshal(sinfo)
		if err != nil {
			return err
		}
		if _, err = os.Stdout.Write(j); err != nil {
			return err
		}
		if _, err = os.Stdout.Write([]byte{'\n'}); err != nil {
			return err
		}
		keepGoing = ok
	}
	return nil
}

/*
	Versioned migration and validation functions
*/

func migrateV1ToV2(ctx context.Context, stateRootIn cid.Cid, cacheRootStr string, store cbornode.IpldStore, height abi.ChainEpoch, log *lib.MigrationLogger) (cid.Cid, time.Duration, func() error, error) {
	start := time.Now()
	stateRootOut, err := migration7.MigrateStateTree(ctx, store, stateRootIn, height, migration7.DefaultConfig())
	duration := time.Since(start)
	cacheWriteCallback := func() error { return nil }
	return stateRootOut, duration, cacheWriteCallback, err
}

func migrateV2ToV3(ctx context.Context, stateRootIn cid.Cid, cacheRootStr string, store cbornode.IpldStore, height abi.ChainEpoch, log *lib.MigrationLogger) (cid.Cid, time.Duration, func() error, error) {
	cfg := migration10.Config{
		MaxWorkers:        8,
		JobQueueSize:      1000,
		ResultQueueSize:   100,
		ProgressLogPeriod: 5 * time.Minute,
	}
	cache := migration10.NewMemMigrationCache()
	if cacheRootStr != "" {
		cacheStateRoot, err := cid.Decode(cacheRootStr)
		if err != nil {
			return cid.Undef, time.Duration(0), nil, err
		}
		cache, err = lib.LoadCache(cacheStateRoot)
		if err != nil {
			return cid.Undef, time.Duration(0), nil, err
		}
		fmt.Printf("read cache from %s/%s\n", lib.EntCachePath, cacheStateRoot)
	}

	start := time.Now()
	stateRootOut, err := migration10.MigrateStateTree(ctx, store, stateRootIn, height, cfg, log, cache)
	if err != nil {
		return cid.Undef, time.Duration(0), nil, err
	}
	duration := time.Since(start)
	cacheWriteCallback := func() error {
		persistStart := time.Now()
		if err := lib.PersistCache(stateRootIn, cache); err != nil {
			return err
		}
		persistDuration := time.Since(persistStart)
		fmt.Printf("cache written to %s/%s, write time: %v\n", lib.EntCachePath, stateRootIn, persistDuration)
		return nil
	}
	return stateRootOut, duration, cacheWriteCallback, nil
}

func migrateV3ToV4(ctx context.Context, stateRootIn cid.Cid, cacheRootStr string, store cbornode.IpldStore, height abi.ChainEpoch, log *lib.MigrationLogger) (cid.Cid, time.Duration, func() error, error) {
	cfg := migration12.Config{
		MaxWorkers:        8,
		JobQueueSize:      1000,
		ResultQueueSize:   100,
		ProgressLogPeriod: 5 * time.Minute,
	}
	cache := migration10.NewMemMigrationCache()
	if cacheRootStr != "" {
		cacheStateRoot, err := cid.Decode(cacheRootStr)
		if err != nil {
			return cid.Undef, time.Duration(0), nil, err
		}
		cache, err = lib.LoadCache(cacheStateRoot)
		if err != nil {
			return cid.Undef, time.Duration(0), nil, err
		}
		fmt.Printf("read cache from %s/%s\n", lib.EntCachePath, cacheStateRoot)
	}

	start := time.Now()
	stateRootOut, err := migration12.MigrateStateTree(ctx, store, stateRootIn, height, cfg, log, cache)
	if err != nil {
		return cid.Undef, time.Duration(0), nil, err
	}
	duration := time.Since(start)
	cacheWriteCallback := func() error {
		persistStart := time.Now()
		if err := lib.PersistCache(stateRootIn, cache); err != nil {
			return err
		}
		persistDuration := time.Since(persistStart)
		fmt.Printf("cache written to %s/%s, write time: %v\n", lib.EntCachePath, stateRootIn, persistDuration)
		return nil
	}
	return stateRootOut, duration, cacheWriteCallback, nil
}

func migrateV4ToV5(ctx context.Context, stateRootIn cid.Cid, cacheRootStr string, store cbornode.IpldStore, height abi.ChainEpoch, log *lib.MigrationLogger) (cid.Cid, time.Duration, func() error, error) {
	cfg := migration13.Config{
		MaxWorkers:        8,
		JobQueueSize:      1000,
		ResultQueueSize:   100,
		ProgressLogPeriod: 5 * time.Minute,
	}
	cache := migration10.NewMemMigrationCache()
	if cacheRootStr != "" {
		cacheStateRoot, err := cid.Decode(cacheRootStr)
		if err != nil {
			return cid.Undef, time.Duration(0), nil, err
		}
		cache, err = lib.LoadCache(cacheStateRoot)
		if err != nil {
			return cid.Undef, time.Duration(0), nil, err
		}
		fmt.Printf("read cache from %s/%s\n", lib.EntCachePath, cacheStateRoot)
	}
	start := time.Now()
	stateRootOut, err := migration13.MigrateStateTree(ctx, store, stateRootIn, height, cfg, log, cache)
	if err != nil {
		return cid.Undef, time.Duration(0), nil, err
	}
	duration := time.Since(start)
	cacheWriteCallback := func() error {
		persistStart := time.Now()
		if err := lib.PersistCache(stateRootIn, cache); err != nil {
			return err
		}
		persistDuration := time.Since(persistStart)
		fmt.Printf("cache written to %s/%s, write time: %v\n", lib.EntCachePath, stateRootIn, persistDuration)
		return nil
	}
	return stateRootOut, duration, cacheWriteCallback, nil
}

func migrateV5ToV6(ctx context.Context, stateRootIn cid.Cid, cacheRootStr string, store cbornode.IpldStore, height abi.ChainEpoch, log *lib.MigrationLogger) (cid.Cid, time.Duration, func() error, error) {
	cfg := migration14.Config{
		MaxWorkers:        8,
		JobQueueSize:      1000,
		ResultQueueSize:   100,
		ProgressLogPeriod: 5 * time.Minute,
	}
	cache := migration10.NewMemMigrationCache()
	if cacheRootStr != "" {
		cacheStateRoot, err := cid.Decode(cacheRootStr)
		if err != nil {
			return cid.Undef, time.Duration(0), nil, err
		}
		cache, err = lib.LoadCache(cacheStateRoot)
		if err != nil {
			return cid.Undef, time.Duration(0), nil, err
		}
		fmt.Printf("read cache from %s/%s\n", lib.EntCachePath, cacheStateRoot)
	}
	start := time.Now()
	stateRootOut, err := migration14.MigrateStateTree(ctx, store, stateRootIn, height, cfg, log, cache)
	if err != nil {
		return cid.Undef, time.Duration(0), nil, err
	}
	duration := time.Since(start)
	cacheWriteCallback := func() error {
		persistStart := time.Now()
		if err := lib.PersistCache(stateRootIn, cache); err != nil {
			return err
		}
		persistDuration := time.Since(persistStart)
		fmt.Printf("cache written to %s/%s, write time: %v\n", lib.EntCachePath, stateRootIn, persistDuration)
		return nil
	}
	return stateRootOut, duration, cacheWriteCallback, nil
}

func validateV6(ctx context.Context, store cbornode.IpldStore, priorEpoch abi.ChainEpoch, stateRoot cid.Cid, wrapped bool) error {
	var err error
	if wrapped {
		stateRoot, err = loadStateRoot(ctx, store, stateRoot)
		if err != nil {
			return xerrors.Errorf("failed to unwrap state root: %w", err)
		}
	}
	tree, err := states6.LoadTree(adt5.WrapStore(ctx, store), stateRoot)
	if err != nil {
		return xerrors.Errorf("failed to load tree: %w", err)
	}
	expectedBalance := builtin6.TotalFilecoin
	start := time.Now()
	acc, err := states6.CheckStateInvariants(tree, expectedBalance, priorEpoch)
	duration := time.Since(start)
	if err != nil {
		return xerrors.Errorf("failed to check state invariants %w", err)
	}
	if acc.IsEmpty() {
		fmt.Printf("Validation: %s -- no errors -- %v\n", stateRoot, duration)
	} else {
		fmt.Printf("Validation: %s -- with errors -- %v\n%s\n", stateRoot, duration, strings.Join(acc.Messages(), "\n"))
	}
	return nil
}

func validateV5(ctx context.Context, store cbornode.IpldStore, priorEpoch abi.ChainEpoch, stateRoot cid.Cid, wrapped bool) error {
	var err error
	if wrapped {
		stateRoot, err = loadStateRoot(ctx, store, stateRoot)
		if err != nil {
			return xerrors.Errorf("failed to unwrap state root: %w", err)
		}
	}
	tree, err := states5.LoadTree(adt5.WrapStore(ctx, store), stateRoot)
	if err != nil {
		return xerrors.Errorf("failed to load tree: %w", err)
	}
	expectedBalance := builtin5.TotalFilecoin
	start := time.Now()
	acc, err := states5.CheckStateInvariants(tree, expectedBalance, priorEpoch)
	duration := time.Since(start)
	if err != nil {
		return xerrors.Errorf("failed to check state invariants %w", err)
	}
	if acc.IsEmpty() {
		fmt.Printf("Validation: %s -- no errors -- %v\n", stateRoot, duration)
	} else {
		fmt.Printf("Validation: %s -- with errors -- %v\n%s\n", stateRoot, duration, strings.Join(acc.Messages(), "\n"))
	}
	return nil
}

func validateV4(ctx context.Context, store cbornode.IpldStore, priorEpoch abi.ChainEpoch, stateRoot cid.Cid, wrapped bool) error {
	var err error
	if wrapped {
		stateRoot, err = loadStateRoot(ctx, store, stateRoot)
		if err != nil {
			return xerrors.Errorf("failed to unwrap state root: %w", err)
		}
	}
	tree, err := states4.LoadTree(adt4.WrapStore(ctx, store), stateRoot)
	if err != nil {
		return xerrors.Errorf("failed to load tree: %w", err)
	}
	expectedBalance := builtin4.TotalFilecoin
	start := time.Now()
	acc, err := states4.CheckStateInvariants(tree, expectedBalance, priorEpoch)
	duration := time.Since(start)
	if err != nil {
		return xerrors.Errorf("failed to check state invariants %w", err)
	}
	if acc.IsEmpty() {
		fmt.Printf("Validation: %s -- no errors -- %v\n", stateRoot, duration)
	} else {
		fmt.Printf("Validation: %s -- with errors -- %v\n%s\n", stateRoot, duration, strings.Join(acc.Messages(), "\n"))
	}
	return nil
}

func validateV3(ctx context.Context, store cbornode.IpldStore, priorEpoch abi.ChainEpoch, stateRoot cid.Cid, wrapped bool) error {
	var err error
	if wrapped {
		stateRoot, err = loadStateRoot(ctx, store, stateRoot)
		if err != nil {
			return xerrors.Errorf("failed to unwrap state root: %w", err)
		}
	}
	tree, err := states3.LoadTree(adt3.WrapStore(ctx, store), stateRoot)
	if err != nil {
		return xerrors.Errorf("failed to load tree: %w", err)
	}

	expectedBalance := builtin3.TotalFilecoin
	start := time.Now()
	acc, err := states3.CheckStateInvariants(tree, expectedBalance, priorEpoch)
	duration := time.Since(start)
	if err != nil {
		return xerrors.Errorf("failed to check state invariants %w", err)
	}
	if acc.IsEmpty() {
		fmt.Printf("Validation: %s -- no errors -- %v\n", stateRoot, duration)
	} else {
		fmt.Printf("Validation: %s -- with errors -- %v\n%s\n", stateRoot, duration, strings.Join(acc.Messages(), "\n"))
	}
	return nil
}

func validateV2(ctx context.Context, store cbornode.IpldStore, priorEpoch abi.ChainEpoch, stateRoot cid.Cid, wrapped bool) error {
	var err error

	if wrapped {
		stateRoot, err = loadStateRoot(ctx, store, stateRoot)
		if err != nil {
			return xerrors.Errorf("failed to unwrap state root: %w", err)
		}
	}
	tree, err := states2.LoadTree(adt0.WrapStore(ctx, store), stateRoot)
	if err != nil {
		return xerrors.Errorf("failed to load tree: %w", err)
	}
	expectedBalance := builtin2.TotalFilecoin
	start := time.Now()
	acc, err := states2.CheckStateInvariants(tree, expectedBalance, priorEpoch)
	duration := time.Since(start)
	if err != nil {
		return xerrors.Errorf("failed to check state invariants", err)
	}
	if acc.IsEmpty() {
		fmt.Printf("Validation: %s -- no errors -- %v\n", stateRoot, duration)
	} else {
		fmt.Printf("Validation: %s -- with errors -- %v\n%s\n", stateRoot, duration, strings.Join(acc.Messages(), "\n"))
	}
	return nil
}

/* Helpers */

func cpuProfile(c *cli.Context) (func(), error) {
	val := c.String("cpuprofile")
	if val == "" { // flag not set do nothing and defer nothing
		return func() {}, nil
	}

	// val is output path of cpuprofile file
	f, err := os.Create(val)
	if err != nil {
		return nil, err
	}
	err = pprof.StartCPUProfile(f)
	if err != nil {
		return nil, err
	}

	return func() {
		pprof.StopCPUProfile()
		err := f.Close()
		if err != nil {
			fmt.Printf("failed to close cpuprofile file %s: %s\n", val, err)
		}
	}, nil
}

func loadStateTreeV2(ctx context.Context, store cbornode.IpldStore, stateRoot cid.Cid) (*states2.Tree, error) {
	adtStore := adt0.WrapStore(ctx, store)
	stateRoot, err := loadStateRoot(ctx, store, stateRoot)
	if err != nil {
		return nil, err
	}
	return states2.LoadTree(adtStore, stateRoot)
}

func loadStateRoot(ctx context.Context, store cbornode.IpldStore, stateRoot cid.Cid) (cid.Cid, error) {
	var treeTop lib.StateRoot
	err := store.Get(ctx, stateRoot, &treeTop)
	if err != nil {
		return cid.Undef, err
	}
	_, _ = fmt.Fprintf(os.Stderr, "State root version: %v\n", treeTop.Version)
	return treeTop.Actors, nil
}
