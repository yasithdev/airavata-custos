<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Storage

A **storage** is a directory core decides a holder gets on a cluster. A storage connector makes it on a storage system;
the [VAST-Provisioner](../connectors/Storage/VAST-Provisioner/README.md) is the only one so far.

## Targets

Each cluster's `targets`, under `core.storage.clusters` in [`config/custos.yaml`](../config/custos.yaml), say how its
storages of each type are made. The type decides whose storage it is:

| Type | One per | Placeholders |
|---|---|---|
| `user_home`, `user_scratch` | Provisioned account | `{user}` |
| `project`, `project_shared` | Active allocation with a provisioned group | `{allocation}` |
| `project_user` | Active membership of a provisioned account | `{user}` and `{allocation}` |

Custos does not start when a target has another type, its `path` lacks or adds a placeholder, or its `owner` or
`group` adds one. A type without a target gives no storages.

| Key | Purpose |
|---|---|
| `path`, `owner`, `group` | Templates over `{user}`, the account's username, and `{allocation}`, the allocation's group |
| `mode` | Octal permission bits, such as `2770`. Custos does not start when one is not octal. |
| `hard_limit_size`, `hard_limit_inodes` | Optional quota, in bytes and inodes, when `hard_limit_size` is set |

## Storages

Core makes or changes storages and publishes `storage::create` for each, a parent before what is under it:

| When | Storages |
|---|---|
| An account is provisioned | Its `user_home`, `user_scratch` and `project_user` in each of its projects |
| An allocation's group is provisioned | Its `project` and `project_shared`, then each provisioned member's `project_user` |
| A provisioned account's membership is created or updated | That member's `project_user` |
| A storage connector's sweep | Every one the targets give the cluster's holders that is missing or differs |

| When | Then |
|---|---|
| A user leaves `ACTIVE` | Retires their accounts' `user_home`, `user_scratch` and `project_user` storages |
| An account is deleted | Retires its `user_home`, `user_scratch` and `project_user` storages |
| An allocation is deleted | Retires its `project`, `project_shared` and `project_user` storages |

Making storages also retires those of the same holders the targets no longer give, so a sweep retires every storage
left without a holder. Retiring marks storages `pending_deletion` for an administrator to review, and a provision or
sweep restores them.

| Route | Privilege | Purpose |
|---|---|---|
| `GET /storages` | `core:clusters:read` | List storages with each one's quota; with `usage=true`, also its use, read from the storage connector |
| `DELETE /storages/{id}` | `core:clusters:write` | Remove a pending storage and those under it, once all of them are pending |
| `PUT /storages/{id}` | `core:clusters:write` | Update a storage: set its temporary quota with `quota_bytes`, `quota_until` (at most 90 days ahead) and `reason` |

A temporary quota is audited under `<cluster>:<path>`, so the record outlives the storage. After `quota_until`, the
next sweep restores the target's quota. A storage without a quota of its own counts against the one above it.

## Events

Core publishes these and subscribes to none; a storage connector acts on them.

| Event | Asks the connector to |
|---|---|
| `storage::create` | Make the storage, a parent before what is under it |
| `storage::update` | Bring the storage to its new state; today, its quota |
| `storage::delete` | Remove the storage with what is under it |

A connector applies events after core commits. A delete that fails every retry leaves the directory on the storage
system until `POST /events/deliveries/{id}/retry` delivers it again.
