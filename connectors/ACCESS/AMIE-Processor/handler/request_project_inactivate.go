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

package handler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/codes"

	"github.com/apache/airavata-custos/connectors/ACCESS/AMIE-Processor/model"
	"github.com/apache/airavata-custos/internal/tracing"
	"github.com/apache/airavata-custos/pkg/models"
	"github.com/apache/airavata-custos/pkg/service"
)

type RequestProjectInactivateHandler struct {
	svc        *service.Service
	amieClient AmieClient
	auditSvc   AuditService
}

func NewRequestProjectInactivateHandler(svc *service.Service, amieClient AmieClient, auditSvc AuditService) *RequestProjectInactivateHandler {
	return &RequestProjectInactivateHandler{svc: svc, amieClient: amieClient, auditSvc: auditSvc}
}

func (h *RequestProjectInactivateHandler) SupportsType() string { return "request_project_inactivate" }

func (h *RequestProjectInactivateHandler) Handle(ctx context.Context, tx *sql.Tx, packetJSON map[string]any, packet *model.Packet, eventID string) (err error) {
	ctx, span := tracing.Start(ctx, "amie.handle:"+packet.Type)
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	}()

	body, err := getBody(packetJSON)
	if err != nil {
		return err
	}
	projectID := getString(body, "ProjectID")
	if err := requireText(projectID, "ProjectID"); err != nil {
		return err
	}

	// AMIE carries the Custos project.id we returned in notify_project_create.
	project, err := h.svc.GetProject(ctx, projectID)
	if err != nil {
		if errors.Is(err, service.ErrNotFound) {
			slog.WarnContext(ctx, "request_project_inactivate: project not found in core; skipping",
				"projectID", projectID)
			return h.reply(ctx, tx, packet, eventID, projectID)
		}
		return fmt.Errorf("request_project_inactivate: lookup project: %w", err)
	}

	if _, err := h.svc.UpdateProjectStatusTx(ctx, tx, project.ID, models.ProjectInactive); err != nil {
		return fmt.Errorf("request_project_inactivate: update project status: %w", err)
	}
	if err := h.auditSvc.Log(ctx, tx, packet.ID, eventID, model.AuditInactivateProject, "project", project.ID, ""); err != nil {
		return fmt.Errorf("request_project_inactivate: audit INACTIVATE_PROJECT: %w", err)
	}

	allocations, err := setProjectAllocationsEnabled(ctx, tx, h.svc, project.ID, false,
		describeStatusChange("Inactivated by AMIE request_project_inactivate", getString(body, "Comment")))
	if err != nil {
		return fmt.Errorf("request_project_inactivate: disable allocations: %w", err)
	}
	memberships, err := setMembershipsEnabled(ctx, tx, h.svc, allocations, false, func(string) bool { return true })
	if err != nil {
		return fmt.Errorf("request_project_inactivate: disable memberships: %w", err)
	}
	if err := auditMemberships(ctx, tx, h.auditSvc, packet.ID, eventID, model.AuditInactivateMembership, memberships); err != nil {
		return fmt.Errorf("request_project_inactivate: %w", err)
	}

	return h.reply(ctx, tx, packet, eventID, projectID)
}

func (h *RequestProjectInactivateHandler) reply(ctx context.Context, tx *sql.Tx, packet *model.Packet, eventID, projectID string) error {
	reply := map[string]any{
		"type": "notify_project_inactivate",
		"body": map[string]any{"ProjectID": projectID},
	}
	return sendReply(ctx, tx, h.amieClient, h.auditSvc, packet, eventID, "request_project_inactivate", reply)
}

func describeStatusChange(primary, comment string) string {
	if comment == "" {
		return primary
	}
	return primary + " (" + comment + ")"
}
