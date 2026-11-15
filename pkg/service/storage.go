// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package service

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/airavata-custos/internal/store"
	"github.com/apache/airavata-custos/pkg/events"
	"github.com/apache/airavata-custos/pkg/models"
)

const storageAuditQuotaChanged = "STORAGE_QUOTA_CHANGED"

// StorageUsage reads the bytes used under each storage path on a cluster, keyed by path.
type StorageUsage func(ctx context.Context) (map[string]int64, error)

// ListStorages lists every storage; withUsage also reads each one's use from its cluster's storage connector.
func (s *Service) ListStorages(ctx context.Context, withUsage bool) ([]models.Storage, error) {
	items, err := s.storages.List(ctx)
	if err != nil || !withUsage {
		return items, err
	}
	s.claimedMu.RLock()
	readers := maps.Clone(s.claimedClusters)
	s.claimedMu.RUnlock()
	used := map[string]map[string]int64{}
	for i, st := range items {
		byPath, read := used[st.ComputeClusterID]
		if usage := readers[st.ComputeClusterID]; !read && usage != nil {
			if byPath, err = usage(ctx); err != nil {
				slog.WarnContext(ctx, "storage usage unavailable; storages listed without it", "compute_cluster_id", st.ComputeClusterID, "err", err)
			}
			used[st.ComputeClusterID] = byPath
		}
		if b, ok := byPath[st.Path]; ok {
			items[i].UsedBytes = &b
		}
	}
	return items, nil
}

// SetStorageTargets sets how each cluster's storages of each type are made, keyed by cluster id.
func (s *Service) SetStorageTargets(targets map[string]map[models.StorageType]models.StorageTarget) error {
	for clusterID, clusterTargets := range targets {
		for typ, target := range clusterTargets {
			if !slices.Contains(models.StorageTypes, typ) {
				return fmt.Errorf("%w: cluster %s has unknown storage type %q", ErrInvalidInput, clusterID, typ)
			}
			for placeholder, belongs := range map[string]bool{"{user}": typ.BelongsToUser(), "{allocation}": typ.BelongsToAllocation()} {
				if strings.Contains(target.Path, placeholder) != belongs || !belongs && strings.Contains(target.Owner+target.Group, placeholder) {
					return fmt.Errorf("%w: cluster %s storage target %s uses a placeholder its type lacks, or lacks one it has", ErrInvalidInput, clusterID, typ)
				}
			}
			if _, err := strconv.ParseUint(target.Mode, 8, 12); err != nil {
				return fmt.Errorf("%w: cluster %s storage target %s needs an octal mode, such as \"0700\"", ErrInvalidInput, clusterID, typ)
			}
		}
	}
	s.storageTargets = targets
	return nil
}

// ClaimStorageTargets claims the cluster for the storage connector that makes its storages and reads their usage, and
// returns its targets. Core makes storages only for a claimed cluster.
func (s *Service) ClaimStorageTargets(clusterID string, usage StorageUsage) map[models.StorageType]models.StorageTarget {
	s.claimedMu.Lock()
	defer s.claimedMu.Unlock()
	s.claimedClusters[clusterID] = usage
	return s.storageTargets[clusterID]
}

// EnsureStorages makes a cluster's storages match its targets and holders, and returns its active ones.
func (s *Service) EnsureStorages(ctx context.Context, clusterID string) ([]models.Storage, error) {
	if err := s.inTx(ctx, func(tx *sql.Tx) error { return s.ensureStoragesTx(ctx, tx, store.StorageScope{ClusterID: clusterID}) }); err != nil {
		return nil, err
	}
	storages, err := s.storages.List(ctx)
	return slices.DeleteFunc(storages, func(st models.Storage) bool { return st.ComputeClusterID != clusterID || st.Status != "active" }), err
}

// UpdateStorage puts an administrator's quota in place of the target's until it lapses.
func (s *Service) UpdateStorage(ctx context.Context, id string, bytes int64, until *time.Time, reason, actorID string) error {
	if bytes <= 0 || until == nil || !until.After(time.Now()) || until.After(time.Now().AddDate(0, 0, 90)) {
		return fmt.Errorf("%w: quota must be positive and until within the next 90 days", ErrInvalidInput)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		st, previousBytes, err := s.storages.SetQuota(ctx, tx, id, bytes, *until)
		if err != nil {
			return err
		}
		if st == nil {
			return fmt.Errorf("%w: no storage with a quota of its own", ErrNotFound)
		}
		if err := s.writeAuditTx(ctx, tx, "storage", storageAuditQuotaChanged, st.ComputeClusterID+":"+st.Path,
			map[string]any{"from": previousBytes, "to": bytes, "until": until, "reason": reason, "actor_id": actorID}); err != nil {
			return err
		}
		return s.eventBus.Publish(ctx, tx, events.StorageUpdateEvent, *st)
	})
}

func (s *Service) DeleteStorage(ctx context.Context, id string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		deleted, err := s.storages.Delete(ctx, tx, id)
		if err != nil {
			return err
		}
		if len(deleted) == 0 {
			return fmt.Errorf("%w: no pending storage, or something under it is active", ErrNotFound)
		}
		return s.eventBus.Publish(ctx, tx, events.StorageDeleteEvent, deleted[0]) // the rest are under it
	})
}

// ensureStoragesTx makes the storages the claimed clusters' targets give the scope's holders, retires the scope's
// others, and publishes each one made or changed, a parent first.
func (s *Service) ensureStoragesTx(ctx context.Context, tx *sql.Tx, scope store.StorageScope) error {
	s.claimedMu.RLock()
	claimedClusterIDs := slices.Collect(maps.Keys(s.claimedClusters))
	s.claimedMu.RUnlock()
	if len(claimedClusterIDs) == 0 {
		return nil
	}
	if err := s.storages.LockScope(ctx, tx, scope); err != nil {
		return err
	}
	desired, err := s.desiredStorages(ctx, tx, scope, claimedClusterIDs)
	if err != nil {
		return err
	}
	if err := s.storages.Retire(ctx, tx, scope, claimedClusterIDs, desired); err != nil {
		return err
	}
	for _, st := range desired {
		changed, err := s.storages.Upsert(ctx, tx, st)
		if err != nil {
			return err
		}
		if changed != nil {
			if err := s.eventBus.Publish(ctx, tx, events.StorageCreateEvent, *changed); err != nil {
				return err
			}
		}
	}
	return nil
}

// desiredStorages fills each claimed cluster's target once per holder of its type. A parent comes first.
func (s *Service) desiredStorages(ctx context.Context, tx *sql.Tx, scope store.StorageScope, claimedClusterIDs []string) ([]models.Storage, error) {
	var accounts, allocations []store.StorageHolder
	var err error
	// An allocation's scope holds no account's own storage, and a user's none of an allocation's.
	if scope.AllocationID == "" {
		if accounts, err = s.storages.AccountHolders(ctx, tx, scope); err != nil {
			return nil, err
		}
	}
	if scope.UserID == "" {
		if allocations, err = s.storages.AllocationHolders(ctx, tx, scope); err != nil {
			return nil, err
		}
	}
	members, err := s.storages.MemberHolders(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	holdersOf := func(typ models.StorageType) []store.StorageHolder {
		switch {
		case typ.BelongsToUser() && typ.BelongsToAllocation():
			return members
		case typ.BelongsToUser():
			return accounts
		default:
			return allocations
		}
	}

	var desired []models.Storage
	seenPaths := map[[2]string]bool{}
	for _, clusterID := range claimedClusterIDs {
		if scope.ClusterID != "" && clusterID != scope.ClusterID {
			continue
		}
		for _, typ := range models.StorageTypes {
			target, ok := s.storageTargets[clusterID][typ]
			if !ok {
				continue
			}
			for _, holder := range holdersOf(typ) {
				if holder.ClusterID != clusterID {
					continue
				}
				expand := strings.NewReplacer("{user}", holder.Username, "{allocation}", holder.PosixGroup).Replace
				if key := [2]string{clusterID, expand(target.Path)}; !seenPaths[key] {
					seenPaths[key] = true
					desired = append(desired, models.Storage{ID: newID(), ComputeClusterID: clusterID, Type: typ, Path: key[1], Username: holder.Username,
						AllocationID: holder.AllocationID, AllocationName: holder.AllocationName, StartTime: holder.StartTime, EndTime: holder.EndTime,
						Owner: expand(target.Owner), Group: expand(target.Group), Mode: target.Mode, QuotaBytes: target.HardLimitSize, QuotaInodes: target.HardLimitInodes})
				}
			}
		}
	}
	slices.SortFunc(desired, func(a, b models.Storage) int { return strings.Compare(a.Path, b.Path) })
	return desired, nil
}

// purgeStorages deletes the owners' storages and publishes the deletion of each not under another, since a connector
// removes a storage with what is under it.
func (s *Service) purgeStorages(ctx context.Context, tx *sql.Tx, scope store.StorageScope) error {
	purged, err := s.storages.Purge(ctx, tx, scope)
	if err != nil {
		return err
	}
	var rootPaths []string
	for _, st := range purged {
		if !slices.ContainsFunc(rootPaths, func(rootPath string) bool { return strings.HasPrefix(st.Path, rootPath+"/") }) {
			rootPaths = append(rootPaths, st.Path)
			if err := s.eventBus.Publish(ctx, tx, events.StorageDeleteEvent, st); err != nil {
				return err
			}
		}
	}
	return nil
}
