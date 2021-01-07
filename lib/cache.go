package lib

import (
	"encoding/gob"
	"os"

	migration9 "github.com/filecoin-project/specs-actors/v3/actors/migration/nv9"
	cid "github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
)

// persist and load migration caches
var EntCachePath = "~/.ent/cache/"

func PersistCache(stateRoot cid.Cid, cache migration9.MemMigrationCache) error {
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
	persistMap := make(map[migration9.MigrationCacheKey]cid.Cid)
	cache.MigrationMap.Range(func(k, v interface{}) bool {
		persistMap[k.(migration9.MigrationCacheKey)] = v.(cid.Cid)
		return true
	})
	return cacheEnc.Encode(persistMap)
}

func LoadCache(stateRoot cid.Cid) (migration9.MemMigrationCache, error) {
	cacheFileName, err := homedir.Expand(EntCachePath + stateRoot.String())
	if err != nil {
		return migration9.MemMigrationCache{}, err
	}
	f, err := os.Open(cacheFileName)
	if err != nil {
		return migration9.MemMigrationCache{}, err
	}
	cacheDec := gob.NewDecoder(f)

	persistMap := make(map[migration9.MigrationCacheKey]cid.Cid)
	err = cacheDec.Decode(&persistMap)

	cache := migration9.NewMemMigrationCache()
	for k, v := range persistMap {
		cache.MigrationMap.Store(k, v)
	}
	return cache, err
}
