-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

CREATE TABLE IF NOT EXISTS storages
(
    id                 VARCHAR(255)   NOT NULL,
    compute_cluster_id VARCHAR(255)   NOT NULL,
    type               VARCHAR(32)    NOT NULL DEFAULT '',
    path               TEXT           NOT NULL,
    username           VARCHAR(255)   NOT NULL DEFAULT '',
    allocation_id      VARCHAR(255),
    allocation_name    VARCHAR(255),
    start_time         TIMESTAMPTZ(6),
    end_time           TIMESTAMPTZ(6),
    owner              VARCHAR(255)   NOT NULL,
    "group"            VARCHAR(255)   NOT NULL,
    mode               TEXT           NOT NULL,
    quota_bytes        BIGINT         NOT NULL DEFAULT 0,
    quota_inodes       BIGINT         NOT NULL DEFAULT 0,
    quota_until        TIMESTAMPTZ(6),
    status             VARCHAR(16)    NOT NULL DEFAULT 'active',
    PRIMARY KEY (id),
    CONSTRAINT chk_storages_status CHECK (status IN ('active', 'pending_deletion')),
    CONSTRAINT uq_storages_path UNIQUE (compute_cluster_id, path)
);

-- Moves the folders the VAST connector tracked before storages moved to core.
DO $$ BEGIN
IF to_regclass('vast_folders') IS NOT NULL THEN
    INSERT INTO storages (id, compute_cluster_id, path, username, allocation_id, allocation_name, start_time, end_time,
        owner, "group", mode, quota_bytes, quota_until, status)
    SELECT id, compute_cluster_id, path, CASE owner_kind WHEN 'cluster_user' THEN owner_name ELSE '' END, allocation_id,
        allocation_name, start_time, end_time, split_part(unix_owner, ':', 1), split_part(unix_owner, ':', 2), '',
        COALESCE(quota_override, 0), quota_until, status
    FROM vast_folders
    ON CONFLICT (compute_cluster_id, path) DO NOTHING;
    DROP TABLE IF EXISTS vast_folders, schema_migrations_vast;
END IF;
END $$;
