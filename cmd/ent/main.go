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
	cid "github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/ent/lib"
)

var migrateCmd = &cli.Command{
	Name:        "migrate",
	Description: "migrate a filecoin v1 state root to v2",
	Subcommands: []*cli.Command{
		{
			Name:   "one",
			Usage:  "migrate a single state tree",
			Action: runMigrateOneCmd,
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "preload"},
				&cli.BoolFlag{Name: "validate"},
			},
		},
		{
			Name:   "chain",
			Usage:  "migrate all state trees from given chain head to genesis",
			Action: runMigrateChainCmd,
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "preload"},
				&cli.IntFlag{Name: "skip", Aliases: []string{"k"}},
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
			Name:   "v2",
			Usage:  "validate a single v2 state tree",
			Action: runValidateCmd,
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "preload"},
				&cli.BoolFlag{Name: "unwrapped"},
			},
		},
	},
}

var infoCmd = &cli.Command{
	Name:        "info",
	Description: "report blockchain and state info",
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
			Name:        "ipld-stats",
			Description: "Measure characteristics of state tree HAMTs and AMTs",
			Action:      runIpldStatsCmd,
		},
	},
}

var exportCmd = &cli.Command{
	Name:        "export",
	Description: "export high-cardinality collections",
	Subcommands: []*cli.Command{
		{
			Name:        "sectors",
			Description: "exports all on-chain sectors",
			Action:      runExportSectorsCmd,
		},
	},
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
			exportCmd,
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

func runMigrateOneCmd(c *cli.Context) error {
	if c.Args().Len() != 2 {
		return xerrors.Errorf("not enough args, need state root to migrate and height of state")
	}
	cleanUp, err := cpuProfile(c)
	if err != nil {
		return err
	}
	defer cleanUp()
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

	preloadStr := c.String("preload")
	maybePreload(c.Context, &chn, preloadStr)

	// Migrate State
	store, err := chn.LoadCborStore(c.Context)
	if err != nil {
		return err
	}
	stateRootIn, err := loadStateRoot(c.Context, store, stateRootInRaw)
	if err != nil {
		return err
	}
	start := time.Now()
	stateRootOut, err := migration7.MigrateStateTree(c.Context, store, stateRootIn, height, migration7.DefaultConfig())
	duration := time.Since(start)
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

	if c.Bool("validate") {
		err := validate(c.Context, store, height, stateRootOut, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func runMigrateChainCmd(c *cli.Context) error {
	if !c.Args().Present() {
		return xerrors.Errorf("not enough args, need chain head to migrate")
	}
	cleanUp, err := cpuProfile(c)
	if err != nil {
		return err
	}
	defer cleanUp()
	bcid, err := cid.Decode(c.Args().First())
	if err != nil {
		return err
	}
	chn := lib.Chain{}

	preloadStr := c.String("preload")
	maybePreload(c.Context, &chn, preloadStr)

	iter, err := chn.NewChainStateIterator(c.Context, bcid)
	if err != nil {
		return err
	}
	store, err := chn.LoadCborStore(c.Context)
	if err != nil {
		return err
	}
	k := c.Int("skip")
	for !iter.Done() {
		val := iter.Val()
		if k == 0 || val.Height%int64(k) == int64(0) { // skip every k epochs
			start := time.Now()
			height := abi.ChainEpoch(val.Height)
			stateRoot, err := loadStateRoot(c.Context, store, val.State)
			if err != nil {
				return err
			}
			stateRootOut, err := migration7.MigrateStateTree(c.Context, store, stateRoot, height, migration7.DefaultConfig())
			duration := time.Since(start)
			if err != nil {
				fmt.Printf("%d -- %s => %s !! %v\n", val.Height, val.State, stateRootOut, err)
			} else {
				fmt.Printf("%d -- %s => %s -- %v\n", val.Height, val.State, stateRootOut, duration)
			}
			writeStart := time.Now()
			if err := chn.FlushBufferedState(c.Context, stateRootOut); err != nil {
				fmt.Printf("%s buffer flush failed: %s\n", err, stateRootOut)
			}
			writeDuration := time.Since(writeStart)
			fmt.Printf("%s buffer flush time: %v\n", stateRootOut, writeDuration)

			// Optional Post-Migration State Validation
			if c.Bool("validate") {
				err := validate(c.Context, store, height, stateRootOut, false)
				if err != nil {
					return err
				}
			}
		}

		if err := iter.Step(c.Context); err != nil {
			return err
		}
	}
	return nil
}

func runValidateCmd(c *cli.Context) error {
	if c.Args().Len() != 2 {
		return xerrors.Errorf("wrong numberof args, need state root to migrate and height")
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

	return validate(c.Context, store, height, stateRoot, wrapped)
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

func runIpldStatsCmd(c *cli.Context) error {
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
	tree, err := loadStateTree(c.Context, store, stateRootIn)
	if err != nil {
		return err
	}
	return lib.PrintIpldStats(c.Context, store, tree, true)
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

	tree, err := loadStateTree(c.Context, store, stateRootIn)
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

func maybePreload(ctx context.Context, chn *lib.Chain, preloadStr string) error {
	if preloadStr == "" { // no preload
		return nil
	}

	preloadStateRoot, err := cid.Decode(preloadStr)
	if err != nil {
		return err
	}
	fmt.Printf("start preload of %s\n", preloadStateRoot)
	loadStart := time.Now()
	err = chn.LoadToReadOnlyBuffer(ctx, preloadStateRoot)
	loadDuration := time.Since(loadStart)
	fmt.Printf("%s preload time: %v\n", preloadStateRoot, loadDuration)
	return err
}

func validate(ctx context.Context, store cbornode.IpldStore, priorEpoch abi.ChainEpoch, stateRoot cid.Cid, wrapped bool) error {
	var tree *states2.Tree
	var err error
	if wrapped {
		tree, err = loadStateTree(ctx, store, stateRoot)
		if err != nil {
			return xerrors.Errorf("failed to load tree: %w", err)
		}
	} else {
		tree, err = states2.LoadTree(adt0.WrapStore(ctx, store), stateRoot)
		if err != nil {
			return xerrors.Errorf("failed to load tree: %w", err)
		}
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

func loadStateTree(ctx context.Context, store cbornode.IpldStore, stateRoot cid.Cid) (*states2.Tree, error) {
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
