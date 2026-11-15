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

//go:build integration

package service

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/apache/airavata-custos/internal/store"
	"github.com/apache/airavata-custos/pkg/models"
)

// storageCreates counts the storage::create events published.
const storageCreates = "SELECT COUNT(*) FROM events WHERE event_type = 'storage::create'"

// newStorageCluster makes a cluster with a home, a project and a member storage target.
func newStorageCluster(t *testing.T, svc *Service) *models.ComputeCluster {
	t.Helper()
	cluster, err := svc.CreateComputeCluster(ctx(), &models.ComputeCluster{Name: "c-" + uuid.NewString()[:8]})
	if err == nil {
		err = svc.SetStorageTargets(map[string]map[models.StorageType]models.StorageTarget{cluster.ID: {
			models.StorageUserHome:    {Path: "/home/{user}", Owner: "{user}", Group: "{user}", Mode: "0700", HardLimitSize: 100},
			models.StorageProjectUser: {Path: "/project/{allocation}/{user}", Owner: "{user}", Group: "{allocation}", Mode: "2700"},
			models.StorageProject:     {Path: "/project/{allocation}", Owner: "0", Group: "{allocation}", Mode: "2750"},
		}})
	}
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	svc.ClaimStorageTargets(cluster.ID, func(context.Context) (map[string]int64, error) { return map[string]int64{"/home/jdoe": 7}, nil })
	svc.EventBus().SubscribeStorageCreated("test-subscriber", func(context.Context, models.Storage) error { return nil })
	return cluster
}

// newStorageAllocation makes an allocation of a new project on the cluster.
func newStorageAllocation(t *testing.T, svc *Service, database *sqlx.DB, clusterID string, status models.AllocationStatus) *models.ComputeAllocation {
	t.Helper()
	project, err := svc.CreateProject(ctx(), &models.Project{Title: "p", Origination: "TEST", ProjectPIID: seedUser(t, database, uuid.NewString()+"@example.edu"), Status: models.ProjectActive})
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	alloc, err := svc.CreateComputeAllocation(ctx(), &models.ComputeAllocation{
		Name: "Grant", ProjectID: project.ID, ComputeClusterID: clusterID, Status: status, StartTime: time.Now(), EndTime: time.Now().AddDate(1, 0, 0),
	})
	if err != nil {
		t.Fatalf("create allocation: %v", err)
	}
	return alloc
}

// Make sure a listing reads use from the cluster's storage connector only when asked.
func TestListStoragesWithUsage(t *testing.T) {
	database := setupTestDB(t)
	svc := newTestService(database)
	cluster := newStorageCluster(t, svc)
	cu, err := svc.CreateComputeClusterUser(ctx(), &models.ComputeClusterUser{ComputeClusterID: cluster.ID, UserID: seedUser(t, database, uuid.NewString()+"@example.edu"), LocalUsername: "jdoe"})
	if err == nil {
		err = svc.MarkComputeClusterUserProvisioned(ctx(), cu.ID)
	}
	if err != nil {
		t.Fatalf("provision account: %v", err)
	}
	for withUsage, want := range map[bool]int64{false: 0, true: 7} {
		list, err := svc.ListStorages(ctx(), withUsage)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		i := slices.IndexFunc(list, func(st models.Storage) bool { return st.ComputeClusterID == cluster.ID && st.Path == "/home/jdoe" })
		if i < 0 {
			t.Fatal("no home storage")
		}
		if got := list[i].UsedBytes; (got == nil) != (want == 0) || got != nil && *got != want {
			t.Errorf("withUsage=%v: used %v, want %d", withUsage, got, want)
		}
	}
}

// Make sure a storage is made from its target for its owner, a retired owner's is active again once ensured, a storage
// is deleted only once everything under it is pending, and deleting an owner purges its storages.
func TestStorageLifecycle(t *testing.T) {
	database := setupTestDB(t)
	svc := newTestService(database)
	cluster := newStorageCluster(t, svc)
	project, err := svc.CreateProject(ctx(), &models.Project{Title: "p", Origination: "TEST", ProjectPIID: seedUser(t, database, uuid.NewString()+"@example.edu"), Status: models.ProjectActive})
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	// The second allocation's group extends the first's, so its storage is beside the first's and not under it.
	allocation := func() *models.ComputeAllocation {
		a, err := svc.CreateComputeAllocation(ctx(), &models.ComputeAllocation{
			Name: "Grant", ProjectID: project.ID, ComputeClusterID: cluster.ID, Status: models.ACTIVE, StartTime: time.Now(), EndTime: time.Now().AddDate(1, 0, 0),
		})
		if err == nil {
			err = svc.MarkComputeAllocationGroupProvisioned(ctx(), a.ID)
		}
		if err != nil {
			t.Fatalf("provision allocation: %v", err)
		}
		return a
	}
	a, sibling := allocation(), allocation()
	projectPath, siblingPath := "/project/"+*a.PosixGroup, "/project/"+*sibling.PosixGroup
	ensure := func() int {
		before := countRows(t, database, storageCreates)
		if _, err := svc.EnsureStorages(ctx(), cluster.ID); err != nil {
			t.Fatalf("ensure: %v", err)
		}
		return countRows(t, database, storageCreates) - before
	}
	user := seedUser(t, database, uuid.NewString()+"@example.edu")
	cu, err := svc.CreateComputeClusterUser(ctx(), &models.ComputeClusterUser{ComputeClusterID: cluster.ID, UserID: user, LocalUsername: "jdoe", ApprovalStatus: models.ClusterAccountApproved, ReviewedBy: &user})
	if err == nil {
		err = svc.MarkComputeClusterUserProvisioned(ctx(), cu.ID)
	}
	if err != nil {
		t.Fatalf("provision account: %v", err)
	}
	if _, err := svc.CreateComputeAllocationMembership(ctx(), &models.ComputeAllocationMembership{ComputeAllocationID: a.ID, UserID: cu.UserID}); err != nil {
		t.Fatalf("create membership: %v", err)
	}
	byPath := func() map[string]models.Storage {
		list, err := svc.ListStorages(ctx(), false)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		got := map[string]models.Storage{}
		for _, st := range list {
			got[st.Path] = st
		}
		return got
	}
	// setAllocationStatus sets the first allocation's status, ensures, and returns how many storages that made or changed.
	setAllocationStatus := func(status models.AllocationStatus) int {
		before := countRows(t, database, storageCreates)
		a.Status = status
		if err := svc.UpdateComputeAllocation(ctx(), a); err != nil {
			t.Fatalf("set allocation %s: %v", status, err)
		}
		ensure()
		return countRows(t, database, storageCreates) - before
	}

	if m := byPath()[projectPath+"/jdoe"]; m.Username != "jdoe" || m.AllocationID == nil || *m.AllocationID != a.ID || m.Group != *a.PosixGroup || m.Mode != "2700" {
		t.Fatalf("member storage: %+v", m)
	}
	until := time.Now().Add(time.Hour)
	if err := svc.UpdateStorage(ctx(), byPath()["/home/jdoe"].ID, 500, &until, "", "admin"); err != nil || ensure() != 0 || byPath()["/home/jdoe"].QuotaBytes != 500 {
		t.Fatalf("administrator's quota: err=%v, want it to stand and nothing to change when storages are ensured again", err)
	}

	setAllocationStatus(models.INACTIVE)
	if got := byPath(); got[projectPath+"/jdoe"].Status != "pending_deletion" || got[siblingPath].Status != "active" {
		t.Fatalf("after retiring: %+v", got)
	}
	if back := setAllocationStatus(models.ACTIVE); back != 2 {
		t.Fatalf("ensuring retired storages: made %d, want the project's and the member's active again", back)
	}
	if err := svc.DeleteStorage(ctx(), byPath()[projectPath].ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete over an active storage: got %v, want ErrNotFound", err)
	}
	setAllocationStatus(models.INACTIVE)
	svc.EventBus().SubscribeStorageDeleted("test-subscriber", func(context.Context, models.Storage) error { return nil })
	if err := svc.DeleteStorage(ctx(), byPath()[projectPath].ID); err != nil || len(byPath()) != 2 ||
		countRows(t, database, "SELECT COUNT(*) FROM events WHERE event_type = 'storage::delete'") != 1 {
		t.Fatalf("delete: err=%v left=%+v, want one delete event for the storage, which takes the member's under it", err, byPath())
	}
	if err := svc.UpdateStorage(ctx(), byPath()[siblingPath].ID, 1, &until, "", "admin"); !errors.Is(err, ErrNotFound) {
		t.Errorf("quota on a storage without one of its own: got %v, want ErrNotFound", err)
	}
	if err := svc.DeleteComputeClusterUser(ctx(), cu.ID); !errors.Is(err, ErrInUse) {
		t.Fatalf("deleting a provisioned account: got %v, want ErrInUse", err)
	}
	_, err = svc.UpdateUserStatus(ctx(), user, models.UserSuspended)
	if _, e := svc.SetComputeAllocationEnabled(ctx(), sibling.ID, false); errors.Join(err, e) != nil ||
		byPath()["/home/jdoe"].Status != "pending_deletion" || byPath()[siblingPath].Status != "pending_deletion" {
		t.Fatalf("suspending the user and deactivating the allocation: err=%v left=%+v, want theirs retired", errors.Join(err, e), byPath())
	}
	err = errors.Join(svc.DeleteComputeClusterUser(ctx(), cu.ID), svc.DeleteComputeAllocation(ctx(), sibling.ID))
	if got := byPath(); err != nil || len(got) != 0 || countRows(t, database, "SELECT COUNT(*) FROM events WHERE event_type = 'storage::delete'") != 3 {
		t.Fatalf("deleting the account and an allocation: err=%v left=%+v, want theirs purged", err, got)
	}
}

// Make sure core creates a storage only once its holder is on the cluster: the account, the active allocation's group,
// or both, and that a member's is retired and purged with its membership.
func TestStoragesFollowProvisioning(t *testing.T) {
	database := setupTestDB(t)
	svc := newTestService(database)
	cluster := newStorageCluster(t, svc)
	alloc := newStorageAllocation(t, svc, database, cluster.ID, models.INACTIVE)
	account := func(username string) *models.ComputeClusterUser {
		cu, err := svc.CreateComputeClusterUser(ctx(), &models.ComputeClusterUser{ComputeClusterID: cluster.ID, UserID: seedUser(t, database, uuid.NewString()+"@example.edu"), LocalUsername: username})
		if err != nil {
			t.Fatalf("create account: %v", err)
		}
		return cu
	}
	join := func(cu *models.ComputeClusterUser) *models.ComputeAllocationMembership {
		m, err := svc.CreateComputeAllocationMembership(ctx(), &models.ComputeAllocationMembership{ComputeAllocationID: alloc.ID, UserID: cu.UserID})
		if err != nil {
			t.Fatalf("create membership: %v", err)
		}
		return m
	}
	check := func(step string, err error, want ...string) {
		var got []string
		if err == nil {
			err = database.Select(&got, `SELECT path FROM storages WHERE compute_cluster_id = $1 ORDER BY path COLLATE "C"`, cluster.ID)
		}
		if n := countRows(t, database, storageCreates); err != nil || !slices.Equal(got, want) || n != len(want) {
			t.Fatalf("%s: err=%v storages=%v events=%d, want %v", step, err, got, n, want)
		}
	}
	projectPath, early, late := "/project/"+*alloc.PosixGroup, account("early"), account("late")
	join(early)
	check("a member before anything is provisioned", nil)
	check("the account", svc.MarkComputeClusterUserProvisioned(ctx(), early.ID), "/home/early")
	check("the group of an inactive allocation", svc.MarkComputeAllocationGroupProvisioned(ctx(), alloc.ID), "/home/early")
	alloc.Status = models.ACTIVE
	err := svc.UpdateComputeAllocation(ctx(), alloc)
	check("the allocation becoming active", err, "/home/early", projectPath, projectPath+"/early")
	_, err = svc.EnsureStorages(ctx(), cluster.ID)
	check("the sweep", err, "/home/early", projectPath, projectPath+"/early")
	err = svc.MarkComputeClusterUserProvisioned(ctx(), late.ID)
	member := join(late)
	check("a provisioned account joining", err, "/home/early", "/home/late", projectPath, projectPath+"/early", projectPath+"/late")
	if desired, err := svc.EnsureStorages(ctx(), cluster.ID); err != nil || len(desired) != 5 || countRows(t, database, storageCreates) != 5 {
		t.Fatalf("ensuring again: err=%v desired=%d, want the five storages and nothing made or changed", err, len(desired))
	}
	var retired []string
	_, err = svc.SetMembershipEnabled(ctx(), member.ID, false)
	if err == nil {
		err = database.Select(&retired, `SELECT path FROM storages WHERE status = 'pending_deletion'`)
	}
	if err != nil || !slices.Equal(retired, []string{projectPath + "/late"}) {
		t.Fatalf("the membership leaving: err=%v retired=%v, want only the member's", err, retired)
	}
	svc.EventBus().SubscribeStorageDeleted("test-subscriber", func(context.Context, models.Storage) error { return nil })
	if err := svc.DeleteComputeAllocationMembership(ctx(), member.ID); err != nil || countRows(t, database, "SELECT COUNT(*) FROM storages") != 4 ||
		countRows(t, database, "SELECT COUNT(*) FROM events WHERE event_type = 'storage::delete'") != 1 {
		t.Fatalf("deleting the membership: err=%v, want the member's storage purged", err)
	}
}

// Make sure an account provisioned while its member storage's other owner is being made still gets that storage.
func TestMemberStorageSurvivesConcurrentProvisioning(t *testing.T) {
	for name, open := range map[string]func(svc *Service, tx *sql.Tx, alloc *models.ComputeAllocation, cu *models.ComputeClusterUser) error{
		"group provisioned": func(svc *Service, tx *sql.Tx, alloc *models.ComputeAllocation, cu *models.ComputeClusterUser) error {
			if _, err := svc.CreateComputeAllocationMembership(ctx(), &models.ComputeAllocationMembership{ComputeAllocationID: alloc.ID, UserID: cu.UserID}); err != nil {
				return err
			}
			return svc.markGroupProvisionedTx(ctx(), tx, alloc)
		},
		"membership added": func(svc *Service, tx *sql.Tx, alloc *models.ComputeAllocation, cu *models.ComputeClusterUser) error {
			if err := svc.MarkComputeAllocationGroupProvisioned(ctx(), alloc.ID); err != nil {
				return err
			}
			m := &models.ComputeAllocationMembership{ID: newID(), ComputeAllocationID: alloc.ID, UserID: cu.UserID, MembershipStatus: models.ACTIVE}
			if err := svc.memberships.Create(ctx(), tx, m); err != nil {
				return err
			}
			return svc.ensureStoragesTx(ctx(), tx, store.StorageScope{UserID: m.UserID, AllocationID: m.ComputeAllocationID})
		},
	} {
		t.Run(name, func(t *testing.T) {
			database := setupTestDB(t)
			svc := newTestService(database)
			cluster := newStorageCluster(t, svc)
			alloc := newStorageAllocation(t, svc, database, cluster.ID, models.ACTIVE)
			cu, err := svc.CreateComputeClusterUser(ctx(), &models.ComputeClusterUser{ComputeClusterID: cluster.ID, UserID: seedUser(t, database, uuid.NewString()+"@example.edu"), LocalUsername: "racer"})
			if err != nil {
				t.Fatalf("seed account: %v", err)
			}
			account := make(chan error, 1)
			if err := svc.inTx(ctx(), func(tx *sql.Tx) error {
				if err := open(svc, tx, alloc, cu); err != nil {
					return err
				}
				go func() { account <- svc.MarkComputeClusterUserProvisioned(ctx(), cu.ID) }()
				time.Sleep(300 * time.Millisecond) // lets the account's transaction reach the locked row while this one is open
				return nil
			}); err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := <-account; err != nil {
				t.Fatalf("provision account: %v", err)
			}
			if n := countRows(t, database, "SELECT COUNT(*) FROM storages WHERE path = $1", "/project/"+*alloc.PosixGroup+"/racer"); n != 1 {
				t.Fatalf("member storage: got %d, want 1", n)
			}
		})
	}
}

// Make sure members added to one allocation at the same moment do not deadlock on the allocation's row.
func TestConcurrentMembershipsDoNotDeadlock(t *testing.T) {
	database := setupTestDB(t)
	svc := newTestService(database)
	cluster := newStorageCluster(t, svc)
	alloc := newStorageAllocation(t, svc, database, cluster.ID, models.ACTIVE)
	const members = 8
	failed := make(chan error, members)
	for range members {
		user := seedUser(t, database, uuid.NewString()+"@example.edu")
		go func() {
			_, err := svc.CreateComputeAllocationMembership(ctx(), &models.ComputeAllocationMembership{ComputeAllocationID: alloc.ID, UserID: user})
			failed <- err
		}()
	}
	for range members {
		if err := <-failed; err != nil {
			t.Errorf("add member: %v", err)
		}
	}
}
