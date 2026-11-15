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

package models

import "time"

// StorageType names what a storage is for, and so whose it is: an account's, an allocation's, or a member's in it.
type StorageType string

const (
	StorageUserHome      StorageType = "user_home"
	StorageUserScratch   StorageType = "user_scratch"
	StorageProject       StorageType = "project"
	StorageProjectShared StorageType = "project_shared"
	StorageProjectUser   StorageType = "project_user"
)

// StorageTypes lists every type; a path two types give goes to the first.
var StorageTypes = []StorageType{StorageUserHome, StorageUserScratch, StorageProject, StorageProjectShared, StorageProjectUser}

// BelongsToUser holds for a type made per account, alone or in an allocation.
func (t StorageType) BelongsToUser() bool {
	return t == StorageUserHome || t == StorageUserScratch || t == StorageProjectUser
}

// BelongsToAllocation holds for a type made per allocation, alone or per member.
func (t StorageType) BelongsToAllocation() bool {
	return t == StorageProject || t == StorageProjectShared || t == StorageProjectUser
}

// StorageTarget says how a cluster's storage of one type is made. Path, Owner and Group are templates over {user},
// the account's username, and {allocation}, the allocation's group. A zero HardLimitSize means no quota of its own,
// so the storage counts against the one above it.
type StorageTarget struct {
	Path            string `yaml:"path"`
	Owner           string `yaml:"owner"`
	Group           string `yaml:"group"`
	Mode            string `yaml:"mode"` // Octal permission bits, such as "0700".
	HardLimitSize   int64  `yaml:"hard_limit_size"`
	HardLimitInodes int64  `yaml:"hard_limit_inodes"`
}

// Storage is one owner's storage on a cluster, as a storage connector makes it. Username and the allocation fields say
// who it is for; the allocation fields are a copy, so they outlive the allocation. A zero quota means none of its own.
type Storage struct {
	ID               string      `json:"id"                 db:"id"`
	ComputeClusterID string      `json:"compute_cluster_id" db:"compute_cluster_id"`
	Type             StorageType `json:"type"               db:"type"`
	Path             string      `json:"path"               db:"path"`
	Username         string      `json:"username"           db:"username"`
	AllocationID     *string     `json:"allocation_id"      db:"allocation_id"`
	AllocationName   *string     `json:"allocation_name"    db:"allocation_name"`
	StartTime        *time.Time  `json:"start_time"         db:"start_time"`
	EndTime          *time.Time  `json:"end_time"           db:"end_time"`
	Owner            string      `json:"owner"              db:"owner"`
	Group            string      `json:"group"              db:"group"`
	Mode             string      `json:"mode"               db:"mode"`
	QuotaBytes       int64       `json:"quota_bytes"        db:"quota_bytes"`
	QuotaInodes      int64       `json:"quota_inodes"       db:"quota_inodes"`
	QuotaUntil       *time.Time  `json:"quota_until"        db:"quota_until"` // Set while an administrator's quota stands in for the target's.
	Status           string      `json:"status"             db:"status"`
	UsedBytes        *int64      `json:"used_bytes,omitempty" db:"-"` // Read from the storage connector when listed with usage.
}
