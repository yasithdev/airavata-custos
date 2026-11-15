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

package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/apache/airavata-custos/pkg/models"
)

// StorageScope narrows storages to a cluster, an account or an allocation. An empty field matches any.
type StorageScope struct {
	ClusterID, UserID, AllocationID string
}

// StorageHolder is who a storage is for: an account's username, an allocation, or both for a membership.
type StorageHolder struct {
	ClusterID      string     `db:"compute_cluster_id"`
	Username       string     `db:"username"`
	AllocationID   *string    `db:"allocation_id"`
	AllocationName *string    `db:"allocation_name"`
	PosixGroup     string     `db:"posix_group"`
	StartTime      *time.Time `db:"start_time"`
	EndTime        *time.Time `db:"end_time"`
}

const (
	allocationHolderColumns = `a.compute_cluster_id, a.id AS allocation_id, a.name AS allocation_name, a.posix_group, a.start_time, a.end_time`
	// allocationGivesStorage holds for an active allocation a with a provisioned group.
	allocationGivesStorage = `a.status = 'ACTIVE' AND a.posix_group IS NOT NULL AND a.group_provisioned_at IS NOT NULL`
)

// storageInScope matches storage s to the scope's cluster $1, user $2 and allocation $3.
const storageInScope = `($1 = '' OR s.compute_cluster_id = $1) AND ($3 = '' OR s.allocation_id = $3)
	AND ($2 = '' OR s.username IN (SELECT local_username FROM compute_cluster_users WHERE user_id = $2 AND compute_cluster_id = s.compute_cluster_id))`

type pgStorageStore struct {
	db *sqlx.DB
}

// NewStorageStore returns a PostgreSQL-backed StorageStore.
func NewStorageStore(db *sqlx.DB) StorageStore {
	return &pgStorageStore{db: db}
}

func (s *pgStorageStore) List(ctx context.Context) ([]models.Storage, error) {
	storages := []models.Storage{}
	err := s.db.SelectContext(ctx, &storages, `SELECT * FROM storages ORDER BY status <> 'pending_deletion', path`)
	return storages, err
}

func (s *pgStorageStore) LockScope(ctx context.Context, tx *sql.Tx, scope StorageScope) error {
	// Not the keys: a membership being inserted holds a key share on its allocation.
	for _, lock := range []string{`SELECT 1 FROM compute_cluster_users cu
		WHERE cu.user_id = $2 AND ($1 = '' OR cu.compute_cluster_id = $1)
			AND ($3 = '' OR cu.compute_cluster_id = (SELECT compute_cluster_id FROM compute_allocations WHERE id = $3))
		ORDER BY cu.id FOR NO KEY UPDATE`, `SELECT 1 FROM compute_allocations a
		WHERE ($1 = '' OR a.compute_cluster_id = $1) AND ($3 = '' OR a.id = $3)
			AND ($3 <> '' OR EXISTS (SELECT 1 FROM compute_allocation_memberships m WHERE m.compute_allocation_id = a.id AND m.user_id = $2))
		ORDER BY a.id FOR NO KEY UPDATE OF a`} {
		if _, err := tx.ExecContext(ctx, lock, scope.ClusterID, scope.UserID, scope.AllocationID); err != nil {
			return err
		}
	}
	return nil
}

func (s *pgStorageStore) AccountHolders(ctx context.Context, tx *sql.Tx, scope StorageScope) (holders []StorageHolder, err error) {
	err = (&sqlx.Tx{Tx: tx, Mapper: s.db.Mapper}).SelectContext(ctx, &holders, `SELECT compute_cluster_id, local_username AS username FROM compute_cluster_users
		WHERE provisioned_at IS NOT NULL AND ($1 = '' OR compute_cluster_id = $1) AND ($2 = '' OR user_id = $2)`, scope.ClusterID, scope.UserID)
	return
}

func (s *pgStorageStore) AllocationHolders(ctx context.Context, tx *sql.Tx, scope StorageScope) (holders []StorageHolder, err error) {
	err = (&sqlx.Tx{Tx: tx, Mapper: s.db.Mapper}).SelectContext(ctx, &holders, `SELECT `+allocationHolderColumns+` FROM compute_allocations a
		WHERE `+allocationGivesStorage+` AND ($1 = '' OR a.compute_cluster_id = $1) AND ($2 = '' OR a.id = $2)`, scope.ClusterID, scope.AllocationID)
	return
}

func (s *pgStorageStore) MemberHolders(ctx context.Context, tx *sql.Tx, scope StorageScope) (holders []StorageHolder, err error) {
	err = (&sqlx.Tx{Tx: tx, Mapper: s.db.Mapper}).SelectContext(ctx, &holders, `SELECT cu.local_username AS username, `+allocationHolderColumns+`
		FROM compute_allocation_memberships m
		JOIN compute_allocations a ON a.id = m.compute_allocation_id AND `+allocationGivesStorage+`
		JOIN compute_cluster_users cu ON cu.compute_cluster_id = a.compute_cluster_id AND cu.user_id = m.user_id AND cu.provisioned_at IS NOT NULL
		WHERE m.membership_status = 'ACTIVE' AND ($1 = '' OR a.compute_cluster_id = $1) AND ($2 = '' OR m.user_id = $2) AND ($3 = '' OR a.id = $3)`,
		scope.ClusterID, scope.UserID, scope.AllocationID)
	return
}

func (s *pgStorageStore) Retire(ctx context.Context, tx *sql.Tx, scope StorageScope, claimedClusterIDs []string, desired []models.Storage) error {
	var desiredClusterIDs, desiredPaths []string
	for _, st := range desired {
		desiredClusterIDs, desiredPaths = append(desiredClusterIDs, st.ComputeClusterID), append(desiredPaths, st.Path)
	}
	_, err := tx.ExecContext(ctx, `UPDATE storages s SET status = 'pending_deletion'
		WHERE s.status = 'active' AND s.compute_cluster_id = ANY($4) AND `+storageInScope+`
			AND (s.compute_cluster_id, s.path) NOT IN (SELECT * FROM unnest($5::text[], $6::text[]))`,
		scope.ClusterID, scope.UserID, scope.AllocationID, claimedClusterIDs, desiredClusterIDs, desiredPaths)
	return err
}

func (s *pgStorageStore) Upsert(ctx context.Context, tx *sql.Tx, st models.Storage) (*models.Storage, error) {
	query, args, err := s.db.BindNamed(`INSERT INTO storages AS s (id, compute_cluster_id, type, path, username, allocation_id,
		allocation_name, start_time, end_time, owner, "group", mode, quota_bytes, quota_inodes)
	VALUES (:id, :compute_cluster_id, :type, :path, :username, :allocation_id, :allocation_name, :start_time, :end_time, :owner, :group,
		:mode, :quota_bytes, :quota_inodes)
	ON CONFLICT (compute_cluster_id, path) DO UPDATE SET type = EXCLUDED.type, username = EXCLUDED.username,
		allocation_id = EXCLUDED.allocation_id, allocation_name = EXCLUDED.allocation_name, start_time = EXCLUDED.start_time,
		end_time = EXCLUDED.end_time, owner = EXCLUDED.owner, "group" = EXCLUDED."group", mode = EXCLUDED.mode,
		quota_bytes = CASE WHEN s.quota_until > now() THEN s.quota_bytes ELSE EXCLUDED.quota_bytes END,
		quota_until = CASE WHEN s.quota_until > now() THEN s.quota_until END,
		quota_inodes = EXCLUDED.quota_inodes, status = 'active'
	WHERE s.status <> 'active' OR s.quota_until <= now() OR s.quota_until IS NULL AND s.quota_bytes <> EXCLUDED.quota_bytes
		OR (s.type, s.username, s.allocation_id, s.allocation_name, s.start_time, s.end_time, s.owner, s."group", s.mode, s.quota_inodes)
			IS DISTINCT FROM (EXCLUDED.type, EXCLUDED.username, EXCLUDED.allocation_id, EXCLUDED.allocation_name,
				EXCLUDED.start_time, EXCLUDED.end_time, EXCLUDED.owner, EXCLUDED."group", EXCLUDED.mode, EXCLUDED.quota_inodes)
	RETURNING *`, st)
	if err != nil {
		return nil, err
	}
	var row models.Storage
	if err := (&sqlx.Tx{Tx: tx, Mapper: s.db.Mapper}).GetContext(ctx, &row, query, args...); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &row, nil
}

func (s *pgStorageStore) SetQuota(ctx context.Context, tx *sql.Tx, id string, bytes int64, until time.Time) (*models.Storage, int64, error) {
	var row struct {
		models.Storage
		PreviousQuotaBytes int64 `db:"previous_quota_bytes"`
	}
	err := (&sqlx.Tx{Tx: tx, Mapper: s.db.Mapper}).GetContext(ctx, &row, `UPDATE storages s SET quota_bytes = $2, quota_until = $3
		FROM (SELECT id, quota_bytes FROM storages WHERE id = $1 FOR UPDATE) old
		WHERE s.id = old.id AND old.quota_bytes <> 0 RETURNING s.*, old.quota_bytes AS previous_quota_bytes`, id, bytes, until)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, nil
	}
	return &row.Storage, row.PreviousQuotaBytes, err
}

func (s *pgStorageStore) Delete(ctx context.Context, tx *sql.Tx, id string) (deleted []models.Storage, err error) {
	err = (&sqlx.Tx{Tx: tx, Mapper: s.db.Mapper}).SelectContext(ctx, &deleted, `WITH subtree AS (SELECT s.id, s.status FROM storages s JOIN storages root
			ON s.compute_cluster_id = root.compute_cluster_id AND (s.id = root.id OR starts_with(s.path, root.path || '/')) WHERE root.id = $1)
		, deleted AS (DELETE FROM storages WHERE id IN (SELECT id FROM subtree)
			AND NOT EXISTS (SELECT 1 FROM subtree WHERE status = 'active') RETURNING *)
		SELECT * FROM deleted ORDER BY path COLLATE "C"`, id)
	return
}

func (s *pgStorageStore) Purge(ctx context.Context, tx *sql.Tx, scope StorageScope) (purged []models.Storage, err error) {
	err = (&sqlx.Tx{Tx: tx, Mapper: s.db.Mapper}).SelectContext(ctx, &purged, `WITH deleted AS (DELETE FROM storages s
		WHERE ($2 <> '' OR $3 <> '') AND `+storageInScope+` RETURNING *)
		SELECT * FROM deleted ORDER BY path COLLATE "C"`, scope.ClusterID, scope.UserID, scope.AllocationID)
	return
}
