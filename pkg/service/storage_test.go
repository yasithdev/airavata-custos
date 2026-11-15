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
	"testing"

	"github.com/apache/airavata-custos/pkg/models"
)

// Make sure a target is refused unless its type is known and its path names exactly that type's owner.
func TestSetStorageTargets(t *testing.T) {
	tests := []struct {
		name    string
		typ     models.StorageType
		path    string
		group   string
		wantErr bool
	}{
		{name: "user", typ: models.StorageUserScratch, path: "/scratch/{user}"},
		{name: "member", typ: models.StorageProjectUser, path: "/project/{allocation}/{user}"},
		{name: "unknown type", typ: "cluster", path: "/shared", wantErr: true},
		{name: "member without user", typ: models.StorageProjectUser, path: "/project/{allocation}", wantErr: true},
		{name: "project naming user", typ: models.StorageProject, path: "/project/{user}", wantErr: true},
		{name: "project group naming user", typ: models.StorageProject, path: "/project/{allocation}", group: "{user}", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := (&Service{}).SetStorageTargets(map[string]map[models.StorageType]models.StorageTarget{"c1": {tt.typ: {Path: tt.path, Group: tt.group, Mode: "0700"}}})
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
