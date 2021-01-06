package lib

import (
	"encoding/gob"
	"os"

	migration9 "github.com/filecoin-project/specs-actors/v3/actors/migration/nv9"
	cid "github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
)

// persist and load migration caches
var entCachePath = "~/.ent/cache/"

func PersistCache(stateRoot cid.Cid, cache migration9.MemMigrationCache) error {
	// make ent cache directory if it doesn't already exist
	cacheDirName, err := homedir.Expand(entCachePath[:len(entCachePath)-1])
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cacheDirName, 0777); err != nil {
		return err
	}

	cacheFileName, err := homedir.Expand(entCachePath + stateRoot.String())
	if err != nil {
		return err
	}
	f, err := os.Create(cacheFileName)
	if err != nil {
		return err
	}
	cacheEnc := gob.NewEncoder(f)
	return cacheEnc.Encode(cache)
}

func LoadCache(stateRoot cid.Cid) (migration9.MemMigrationCache, error) {
	cacheFileName, err := homedir.Expand(entCachePath + stateRoot.String())
	if err != nil {
		return migration9.MemMigrationCache{}, err
	}
	f, err := os.Open(cacheFileName)
	if err != nil {
		return migration9.MemMigrationCache{}, err
	}
	cacheDec := gob.NewDecoder(f)
	var cache migration9.MemMigrationCache
	err = cacheDec.Decode(&cache)
	return cache, err
}
