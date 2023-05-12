/*
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.uber.cadence.migration;

import com.uber.cadence.*;
import com.uber.cadence.activity.ActivityOptions;
import com.uber.cadence.client.WorkflowClient;
import com.uber.cadence.common.RetryOptions;
import com.uber.cadence.internal.sync.SyncWorkflowDefinition;
import com.uber.cadence.workflow.*;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;

public class MigrationInterceptor extends WorkflowInterceptorBase {

  // TODO: add new domain override
  private final WorkflowInterceptor next;

  private final MigrationActivities migrationActivities;
  private static final String versionChangeID = "cadenceMigrationInterceptor";
  private static final int versionV1 = 1;
  
  private class MigrationDecision {
    boolean shouldMigrate;
    String reason;

    public MigrationDecision(boolean shouldMigrate, String reason) {
      this.shouldMigrate = shouldMigrate;
      this.reason = reason;
    }
  }

  public MigrationInterceptor(WorkflowInterceptor next) {
    super(next);
    this.next = next;
  }

  @Override
  public byte[] executeWorkflow(
      SyncWorkflowDefinition workflowDefinition, WorkflowExecuteInput input) {

    WorkflowInfo workflowInfo = Workflow.getWorkflowInfo();
    int version = getVersion(versionChangeID, Workflow.DEFAULT_VERSION, versionV1);
    switch (version) {
      case versionV1:
        // Skip migration on non-cron and child workflows
        WorkflowExecutionStartedEventAttributes startedEventAttributes =
            input.getWorkflowExecutionStartedEventAttributes();
        if (!isCronSchedule(startedEventAttributes))
          return next.executeWorkflow(workflowDefinition, input);
        if (isChildWorkflow(startedEventAttributes)) {
          return next.executeWorkflow(workflowDefinition, input);
        }

        MigrationDecision decision =
            Workflow.sideEffect(
                MigrationDecision.class, () -> shouldMigrate(workflowDefinition, input));
        if (decision.shouldMigrate) {
          StartWorkflowExecutionRequest request =
              new StartWorkflowExecutionRequest()
                  .setDomain(workflowInfo.getDomain())
                  .setWorkflowId(workflowInfo.getWorkflowId())
                  .setTaskList(new TaskList().setName(startedEventAttributes.taskList.getName()))
                  .setInput(input.getInput())
                  .setWorkflowType(new WorkflowType().setName(input.getWorkflowType().getName()))
                  .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.TerminateIfRunning)
                  .setRetryPolicy(startedEventAttributes.getRetryPolicy())
                  .setRequestId(UUID.randomUUID().toString())
                  .setIdentity(startedEventAttributes.getIdentity())
                  .setMemo(startedEventAttributes.getMemo())
                  .setCronSchedule(startedEventAttributes.getCronSchedule())
                  .setHeader(startedEventAttributes.getHeader())
                  .setSearchAttributes(startedEventAttributes.getSearchAttributes())
                  .setExecutionStartToCloseTimeoutSeconds(
                      startedEventAttributes.getExecutionStartToCloseTimeoutSeconds())
                  .setTaskStartToCloseTimeoutSeconds(
                      startedEventAttributes.getTaskStartToCloseTimeoutSeconds());

          ActivityOptions options =
              new ActivityOptions.Builder()
                  .setScheduleToCloseTimeout(Duration.ofSeconds(10))
                  .setRetryOptions(new RetryOptions.Builder().build())
                  .build();
          MigrationActivities activities =
              Workflow.newActivityStub(MigrationActivities.class, options);
          try {
            MigrationActivities.StartNewWorkflowExecutionResponse response =
                activities.startWorkflowInNewDomain(
                    new MigrationActivities.StartNewWorkflowRequest());
            // TODO: add metrics and logging
            throw new CancellationException(
                "cancel due to migration:" + response.response.toString());
          } catch (ActivityException e) {
            // fallback if start workflow in new domain failed
            return next.executeWorkflow(workflowDefinition, input);
          }
        }
      default:
        return next.executeWorkflow(workflowDefinition, input);
    }
  }

  private MigrationDecision shouldMigrate(
      SyncWorkflowDefinition workflowDefinition, WorkflowExecuteInput input) {
    return new MigrationDecision(true, "");
  }

  @Override
  public void continueAsNew(
      Optional<String> workflowType, Optional<ContinueAsNewOptions> options, Object[] args) {

    int version = getVersion(versionChangeID, Workflow.DEFAULT_VERSION, versionV1);
    switch (version) {
      case versionV1:
        WorkflowInfo workflowInfo = Workflow.getWorkflowInfo();
        WorkflowExecutionStartedEventAttributes startedEventAttributes =
            workflowInfo.getWorkflowExecutionStartedEventAttributes();
        if (isChildWorkflow(startedEventAttributes)) {
          next.continueAsNew(workflowType, options, args);
        }
        MigrationDecision decision =
            Workflow.sideEffect(MigrationDecision.class, () -> new MigrationDecision(true, ""));
        if (decision.shouldMigrate) {
          StartWorkflowExecutionRequest request =
              new StartWorkflowExecutionRequest()
                  .setDomain(workflowInfo.getDomain())
                  .setWorkflowId(workflowInfo.getWorkflowId())
                  .setTaskList(new TaskList().setName(startedEventAttributes.taskList.getName()))
                  .setInput(workflowInfo.getDataConverter().toData(args))
                  .setWorkflowType(
                      new WorkflowType()
                          .setName(startedEventAttributes.getWorkflowType().getName()))
                  .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.TerminateIfRunning)
                  .setRetryPolicy(startedEventAttributes.getRetryPolicy())
                  .setRequestId(UUID.randomUUID().toString())
                  .setIdentity(startedEventAttributes.getIdentity())
                  .setMemo(startedEventAttributes.getMemo())
                  .setCronSchedule(startedEventAttributes.getCronSchedule())
                  .setHeader(startedEventAttributes.getHeader())
                  .setSearchAttributes(startedEventAttributes.getSearchAttributes())
                  .setExecutionStartToCloseTimeoutSeconds(
                      startedEventAttributes.getExecutionStartToCloseTimeoutSeconds())
                  .setTaskStartToCloseTimeoutSeconds(
                      startedEventAttributes.getTaskStartToCloseTimeoutSeconds());
          ActivityOptions activityOptions =
              new ActivityOptions.Builder()
                  .setScheduleToCloseTimeout(Duration.ofSeconds(10))
                  .setRetryOptions(new RetryOptions.Builder().build())
                  .build();
          MigrationActivities activities =
              Workflow.newActivityStub(MigrationActivities.class, activityOptions);
          try {
            MigrationActivities.StartNewWorkflowExecutionResponse response =
                activities.startWorkflowInNewDomain(
                    new MigrationActivities.StartNewWorkflowRequest());
            // TODO: add metrics and logging
            throw new CancellationException(
                "cancel due to migration:" + response.response.toString());
          } catch (ActivityException e) {
            // fallback if start workflow in new domain failed
            next.continueAsNew(workflowType, options, args);
          }
        }
      default:
        next.continueAsNew(workflowType, options, args);
    }
  }

  public boolean isChildWorkflow(WorkflowExecutionStartedEventAttributes startedEventAttributes) {
    return startedEventAttributes.isSetParentWorkflowExecution()
        && !startedEventAttributes.getParentWorkflowExecution().isSetWorkflowId();
  }

  public boolean isCronSchedule(WorkflowExecutionStartedEventAttributes startedEventAttributes) {
    return startedEventAttributes.isSetCronSchedule()
        || !startedEventAttributes.cronSchedule.equals("");
  }
}
