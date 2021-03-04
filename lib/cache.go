package lib

import (
	"encoding/gob"
	"os"

	migration10 "github.com/filecoin-project/specs-actors/v3/actors/migration/nv10"
	cid "github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
)

// persist and load migration caches
var EntCachePath = "~/.ent/cache/"

func PersistCache(stateRoot cid.Cid, cache *migration10.MemMigrationCache) error {
	// make ent cache directory if it doesn't already exist
	cacheDirName, err := homedir.Expand(EntCachePath[:len(EntCachePath)-1])
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cacheDirName, 0777); err != nil {
		return err
	}

	cacheFileName, err := homedir.Expand(EntCachePath + stateRoot.String())
	if err != nil {
		return err
	}
	f, err := os.Create(cacheFileName)
	if err != nil {
		return err
	}
	cacheEnc := gob.NewEncoder(f)
	persistMap := make(map[string]cid.Cid)
	cache.MigrationMap.Range(func(k, v interface{}) bool {
		persistMap[k.(string)] = v.(cid.Cid)
		return true
	})
	return cacheEnc.Encode(persistMap)
}

func LoadCache(stateRoot cid.Cid) (*migration10.MemMigrationCache, error) {
	cacheFileName, err := homedir.Expand(EntCachePath + stateRoot.String())
	if err != nil {
		return nil, err
	}
	f, err := os.Open(cacheFileName)
	if err != nil {
		return nil, err
	}
	cacheDec := gob.NewDecoder(f)

	persistMap := make(map[string]cid.Cid)
	err = cacheDec.Decode(&persistMap)

	cache := migration10.NewMemMigrationCache()
	for k, v := range persistMap {
		cache.MigrationMap.Store(k, v)
	}
	return cache, err
}
