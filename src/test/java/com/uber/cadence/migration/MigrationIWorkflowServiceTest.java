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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.uber.cadence.*;
import com.uber.cadence.client.WorkflowClient;
import com.uber.cadence.client.WorkflowOptions;
import com.uber.cadence.testing.TestWorkflowEnvironment;
import com.uber.cadence.worker.Worker;
import com.uber.cadence.workflow.WorkflowMethod;
import java.util.stream.Stream;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MigrationIWorkflowServiceTest {

  static final String TASK_LIST = "tasklist";
  static final String WORKFLOW_NAME = "SimpleWF";

  static final byte[] NEXT_PAGE_TOKEN = {};

  private TestWorkflowEnvironment testEnvNew;
  private Worker workerNew;
  private WorkflowClient clientNew;

  private TestWorkflowEnvironment testEnvOld;
  private Worker workerOld;
  private WorkflowClient clientOld;

  private MigrationIWorkflowService migrationService;

  public interface SimpleW {
    @WorkflowMethod(
      name = WORKFLOW_NAME,
      executionStartToCloseTimeoutSeconds = 10,
      taskList = TASK_LIST
    )
    void run();
  }

  public static class SimpleWImpl implements SimpleW {
    @Override
    public void run() {}
  }

  @Before
  public void setUp() {
    testEnvNew = TestWorkflowEnvironment.newInstance();
    clientNew = testEnvNew.newWorkflowClient();
    workerNew = testEnvNew.newWorker(TASK_LIST);
    workerNew.registerWorkflowImplementationTypes(SimpleWImpl.class);

    testEnvOld = TestWorkflowEnvironment.newInstance();
    clientOld = testEnvOld.newWorkflowClient();
    workerOld = testEnvOld.newWorker(TASK_LIST);
    workerOld.registerWorkflowImplementationTypes(SimpleWImpl.class);

    testEnvNew.start();
    testEnvOld.start();

    migrationService =
        new MigrationIWorkflowService(
            testEnvOld.getWorkflowService(),
            testEnvOld.getDomain(),
            testEnvNew.getWorkflowService(),
            testEnvNew.getDomain());

    // start and run some workflows in both domains
    try {
      runSyncWF(clientNew, "wf-completed-only-in-new");
      runSyncWF(clientOld, "wf-completed-only-in-old");
      runSyncWF(clientNew, "wf-completed-in-both");
      runSyncWF(clientOld, "wf-completed-in-both");
    } catch (Exception e) {
      fail("no exception excepted in starting workflows:" + e.getMessage());
    }
  }

  @After
  public void tearDown() {
    testEnvNew.close();
    testEnvOld.close();
  }

  @Test
  public void testStartWorkflowExecutionWithNewWorkflow() {
    try {
      StartWorkflowExecutionResponse response =
          migrationService.StartWorkflowExecution(
              new StartWorkflowExecutionRequest()
                  .setRequestId("requestID")
                  .setDomain(testEnvOld.getDomain())
                  .setTaskList(new TaskList().setName(TASK_LIST))
                  .setWorkflowType(new WorkflowType().setName(WORKFLOW_NAME))
                  .setWorkflowId("wf-new")
                  .setExecutionStartToCloseTimeoutSeconds(1)
                  .setTaskStartToCloseTimeoutSeconds(10));
      assertNumOfExecutions(testEnvNew, "wf-new", 1);
      assertNumOfExecutions(testEnvOld, "wf-new", 0);
    } catch (TException e) {
      fail("should not throw error on start workflow" + e.getMessage());
    }
  }

  @Test
  public void testStartWorkflowExecutionWithOldWorkflow() {
    try {
      StartWorkflowExecutionResponse response =
          migrationService.StartWorkflowExecution(
              new StartWorkflowExecutionRequest()
                  .setRequestId("requestID")
                  .setDomain(testEnvOld.getDomain())
                  .setTaskList(new TaskList().setName(TASK_LIST))
                  .setWorkflowType(new WorkflowType().setName(WORKFLOW_NAME))
                  .setWorkflowId("wf-old")
                  .setExecutionStartToCloseTimeoutSeconds(1)
                  .setTaskStartToCloseTimeoutSeconds(10));
      assertNumOfExecutions(testEnvNew, "wf-old", 1);
      assertNumOfExecutions(testEnvOld, "wf-old", 0);
    } catch (TException e) {
      fail("should not throw error on start workflow" + e.getMessage());
    }
  }

  @Test
  public void testSignalWithStartWorkflowExecutionWithNewWorkflow() {
    try {
      StartWorkflowExecutionResponse response =
          migrationService.SignalWithStartWorkflowExecution(
              new SignalWithStartWorkflowExecutionRequest()
                  .setRequestId("requestID")
                  .setDomain(testEnvOld.getDomain())
                  .setTaskList(new TaskList().setName(TASK_LIST))
                  .setSignalName("signalName")
                  .setWorkflowType(new WorkflowType().setName(WORKFLOW_NAME))
                  .setWorkflowId("wf-new")
                  .setExecutionStartToCloseTimeoutSeconds(1)
                  .setTaskStartToCloseTimeoutSeconds(10)
                  .setSignalInput(new byte[] {}));

      assertNumOfExecutions(testEnvNew, "wf-new", 1);
      assertNumOfExecutions(testEnvOld, "wf-new", 0);
    } catch (TException e) {
      fail("should not throw error on signal with start workflow" + e.getMessage());
    }
  }

  @Test
  public void testSignalWithStartWorkflowExecutionWithOldWorkflow() {
    try {
      StartWorkflowExecutionResponse response =
          migrationService.SignalWithStartWorkflowExecution(
              new SignalWithStartWorkflowExecutionRequest()
                  .setRequestId("requestID")
                  .setDomain(testEnvOld.getDomain())
                  .setTaskList(new TaskList().setName(TASK_LIST))
                  .setSignalName("signalName")
                  .setWorkflowType(new WorkflowType().setName(WORKFLOW_NAME))
                  .setWorkflowId("wf-old")
                  .setExecutionStartToCloseTimeoutSeconds(1)
                  .setTaskStartToCloseTimeoutSeconds(10)
                  .setSignalInput(new byte[] {}));

      assertNumOfExecutions(testEnvNew, "wf-old", 1);
      assertNumOfExecutions(testEnvOld, "wf-old", 0);
    } catch (TException e) {
      fail("should not throw error on signal with start workflow" + e.getMessage());
    }
  }

  @Test
  public void testGetWorkflowExecutionHistoryWithCompletedWFInBoth() {
    WorkflowExecution execution = new WorkflowExecution().setWorkflowId("wf-completed-in-both");
    try {
      GetWorkflowExecutionHistoryResponse expectedResponse =
          clientNew
              .getService()
              .GetWorkflowExecutionHistory(
                  new GetWorkflowExecutionHistoryRequest()
                      .setDomain(testEnvNew.getDomain())
                      .setExecution(execution));
      GetWorkflowExecutionHistoryResponse response =
          migrationService.GetWorkflowExecutionHistory(
              new GetWorkflowExecutionHistoryRequest()
                  .setDomain(testEnvOld.getDomain())
                  .setExecution(execution));
      assertEquals(expectedResponse, response);
    } catch (Exception e) {
      fail("should not throw error on get workflow execution on both client");
    }
  }

  @Test
  public void testGetWorkflowExecutionHistoryWithCompletedWFInNew() {

    try {
      WorkflowExecution execution =
          new WorkflowExecution().setWorkflowId("wf-completed-only-in-new");
      GetWorkflowExecutionHistoryResponse response =
          migrationService.GetWorkflowExecutionHistory(
              new GetWorkflowExecutionHistoryRequest()
                  .setDomain(testEnvNew.getDomain())
                  .setExecution(execution));
      assertTrue("Workflow completed in New", response.getHistory().getEvents().size() > 0);
    } catch (Exception e) {
      fail("should not throw error on get workflow execution on new client");
    }
  }

  @Test
  public void testGetWorkflowExecutionHistoryWithCompletedWFInOld() {

    try {
      WorkflowExecution execution =
          new WorkflowExecution().setWorkflowId("wf-completed-only-in-new");
      GetWorkflowExecutionHistoryResponse response =
          migrationService.GetWorkflowExecutionHistory(
              new GetWorkflowExecutionHistoryRequest()
                  .setDomain(testEnvOld.getDomain())
                  .setExecution(execution));
      assertTrue("Workflow completed in Old", response.getHistory().getEvents().size() > 0);
    } catch (Exception e) {
      fail("should not throw error on get workflow execution on new client");
    }
  }

  @Test
  public void testListWorkflowExecutionsOnlyInNew() throws Exception {
    // Create a valid ListWorkflowExecutionsRequest
    try {
      for (int i = 0; i < 100; i++) {
        runSyncWF(clientNew, "wf-completed-only-in-new");
        //          runSyncWF(clientOld, "wf-completed-only-in-old");
        //          runSyncWF(clientNew, "wf-completed-in-both");
        //          runSyncWF(clientOld, "wf-completed-in-both");
      }
      ListWorkflowExecutionsRequest listRequest =
          new ListWorkflowExecutionsRequest()
              .setDomain(testEnvNew.getDomain())
              .setPageSize(10)
              .setNextPageToken(NEXT_PAGE_TOKEN)
              .setQuery("");
      if (migrationService.hasPrefix(listRequest.getNextPageToken(), "to".getBytes())) {

        ListWorkflowExecutionsResponse response =
            migrationService.ListWorkflowExecutions(listRequest);

        assertNotNull(response);
        assertEquals(1000, response.getExecutions().size());
      }
    } catch (Exception e) {
      fail("should not throw error on list workflow executions" + e.getMessage());
    }
  }

  //  @Test
  //  public void testListWorkflowExecutionInBoth() {
  //
  //    try {
  //      for (int i = 0; i < 100; i++) {
  //        runSyncWF(clientNew, "workflow");
  //        runSyncWF(clientOld, "workflow");
  //      }
  //      ListWorkflowExecutionsRequest listRequestInNew =
  //          new ListWorkflowExecutionsRequest()
  //              .setDomain(testEnvNew.getDomain())
  //              .setPageSize(3)
  //              .setQuery("");
  //      ListWorkflowExecutionsRequest listRequestInOld =
  //          new ListWorkflowExecutionsRequest()
  //              .setDomain(testEnvOld.getDomain())
  //              .setPageSize(3)
  //              .setQuery("");
  //
  //      migrationService.ListWorkflowExecutions(listRequestInNew);
  //      migrationService.ListWorkflowExecutions(listRequestInOld);
  //
  //      assertNumOfExecutions(testEnvNew, "workflow", 100);
  //      assertNumOfExecutions(testEnvOld, "workflow", 100);
  //
  //    } catch (Exception e) {
  //      fail(e.getMessage());
  //    }
  //  }

  //  @Test
  //  public void testListWorkflowExecutionsInBoth() {
  //    // Create a valid ListWorkflowExecutionsRequest
  //    try {
  //
  //      ListWorkflowExecutionsResponse expectedResponse =
  //          clientNew
  //              .getService()
  //              .ListWorkflowExecutions(
  //                  new ListWorkflowExecutionsRequest()
  //                      .setDomain(testEnvNew.getDomain())
  //                      .setPageSize(10)
  //                      .setNextPageToken(NEXT_PAGE_TOKEN)
  //                      .setQuery(""));
  //      ListWorkflowExecutionsRequest listRequest =
  //          new ListWorkflowExecutionsRequest()
  //              .setDomain(testEnvOld.getDomain())
  //              .setPageSize(10)
  //              .setNextPageToken(NEXT_PAGE_TOKEN)
  //              .setQuery("");
  //
  //      try {
  //        for (int i = 0; i < 100; i++) {
  //          testEnvOld.getWorkflowService().StartWorkflowExecution(listRequest);
  ////          runSyncWF(clientNew, "wf-completed-only-in-new");
  ////          runSyncWF(clientOld, "wf-completed-only-in-old");
  ////          runSyncWF(clientNew, "wf-completed-in-both");
  ////          runSyncWF(clientOld, "wf-completed-in-both");
  //        }
  //      } catch (Exception e) {
  //        fail("no exception excepted in starting workflows:" + e.getMessage());
  //      }
  //      if (migrationService.hasPrefix(listRequest.getNextPageToken(), "to".getBytes())) {
  //
  //        ListWorkflowExecutionsResponse response =
  //            migrationService.ListWorkflowExecutions(listRequest);
  //
  //        assertNotNull(response);
  //        assertEquals(expectedResponse, response);
  //      }
  //
  //      assertEquals(1000, expectedResponse.getNextPageToken().length);
  //    } catch (Exception e) {
  //      fail("should not throw error on list workflow executions" + e.getMessage());
  //    }
  //  }

  @Test
  public void testScanWorkflowExecutionsOnlyInOld() {
    try {
      ListWorkflowExecutionsRequest listRequest =
          new ListWorkflowExecutionsRequest()
              .setDomain(testEnvOld.getDomain())
              .setPageSize(10)
              .setNextPageToken(NEXT_PAGE_TOKEN)
              .setQuery("workflowType = '" + WORKFLOW_NAME + "'");
      if (migrationService.hasPrefix(listRequest.getNextPageToken(), "to".getBytes())) {
        ListWorkflowExecutionsResponse response =
            migrationService.ScanWorkflowExecutions(listRequest);

        assertEquals(4, response.getExecutions().size());
      }
    } catch (TException e) {
      fail("should not throw error on scan workflow executions" + e.getMessage());
    }
  }

  @Test
  public void testQueryWorkflow() {
    try {
      QueryWorkflowResponse response =
          migrationService.QueryWorkflow(
              new QueryWorkflowRequest()
                  .setDomain(testEnvOld.getDomain())
                  .setExecution(new WorkflowExecution().setWorkflowId("wf-completed-in-both")));
      assertEquals(0, response.getQueryResult().length);
    } catch (TException e) {
      fail("should not throw error on query workflow" + e.getMessage());
    }
  }

  @Test
  public void testCountWorkflowExecutions() {
    try {
      CountWorkflowExecutionsResponse response =
          migrationService.CountWorkflowExecutions(
              new CountWorkflowExecutionsRequest()
                  .setDomain(testEnvOld.getDomain())
                  .setQuery("workflowType = '" + WORKFLOW_NAME + "'"));
      assertEquals(4, response.getCount());
    } catch (TException e) {
      fail("should not throw error on count workflow executions" + e.getMessage());
    }
  }

  private void runSyncWF(WorkflowClient wc, String workflowID) {
    wc.newWorkflowStub(
            SimpleW.class,
            new WorkflowOptions.Builder().setTaskList(TASK_LIST).setWorkflowId(workflowID).build())
        .run();
  }

  private void assertNumOfExecutions(
      TestWorkflowEnvironment env, String workflowID, int expectRuns) {
    try {
      ListClosedWorkflowExecutionsResponse responseClose =
          env.getWorkflowService()
              .ListClosedWorkflowExecutions(
                  new ListClosedWorkflowExecutionsRequest()
                      .setDomain(env.getDomain())
                      .setExecutionFilter(new WorkflowExecutionFilter().setWorkflowId(workflowID)));
      ListOpenWorkflowExecutionsResponse responseOpen =
          env.getWorkflowService()
              .ListOpenWorkflowExecutions(
                  new ListOpenWorkflowExecutionsRequest()
                      .setDomain(env.getDomain())
                      .setExecutionFilter(new WorkflowExecutionFilter().setWorkflowId(workflowID)));
      long totalSize =
          Stream.concat(
                  responseClose.getExecutions().stream(), responseOpen.getExecutions().stream())
              .distinct()
              .count();
      assertEquals(expectRuns, totalSize);
    } catch (Exception e) {
      fail("should not throw error:" + e.getMessage());
    }
  }
}
