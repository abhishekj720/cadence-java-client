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

import com.google.common.base.Strings;
import com.uber.cadence.*;
import com.uber.cadence.serviceclient.IWorkflowService;
import com.uber.cadence.serviceclient.IWorkflowServiceBase;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.thrift.TException;

public class MigrationIWorkflowService extends IWorkflowServiceBase {

  private IWorkflowService serviceOld, serviceNew;
  private String domainOld, domainNew;
  private static final int _defaultPageSize = 10;
  private static final String _listWorkflow = "_listWorkflow";
  private static final String _scanWorkflow = "_scanWorkflow";
  byte[] _marker = "to".getBytes();

  public MigrationIWorkflowService(
      IWorkflowService serviceOld,
      String domainOld,
      IWorkflowService serviceNew,
      String domainNew) {
    this.serviceOld = serviceOld;
    this.domainOld = domainOld;
    this.serviceNew = serviceNew;
    this.domainNew = domainNew;
  }

  @Override
  public StartWorkflowExecutionResponse StartWorkflowExecution(
      StartWorkflowExecutionRequest startRequest) throws TException {

    if (shouldStartInNew(startRequest.getWorkflowId()))
      return serviceNew.StartWorkflowExecution(startRequest);

    return serviceOld.StartWorkflowExecution(startRequest);
  }

  @Override
  public StartWorkflowExecutionResponse SignalWithStartWorkflowExecution(
      SignalWithStartWorkflowExecutionRequest signalWithStartRequest) throws TException {
    if (shouldStartInNew(signalWithStartRequest.getWorkflowId()))
      return serviceNew.SignalWithStartWorkflowExecution(signalWithStartRequest);
    return serviceOld.SignalWithStartWorkflowExecution(signalWithStartRequest);
  }

  @Override
  public GetWorkflowExecutionHistoryResponse GetWorkflowExecutionHistory(
      GetWorkflowExecutionHistoryRequest getRequest) throws TException {
    if (shouldStartInNew(getRequest.execution.getWorkflowId()))
      return serviceNew.GetWorkflowExecutionHistory(getRequest);
    return serviceOld.GetWorkflowExecutionHistory(getRequest);
  }

  private ListWorkflowExecutionsResponse callOldCluster(
      ListWorkflowExecutionsRequest listWorkflowExecutionsRequest,
      int pageSizeOverride,
      String searchType)
      throws TException {

    if (pageSizeOverride != 0) {
      listWorkflowExecutionsRequest.setPageSize(pageSizeOverride);
    }
    ListWorkflowExecutionsResponse response = new ListWorkflowExecutionsResponse();
    if (searchType.equals(_listWorkflow)) {
      response = serviceOld.ListWorkflowExecutions(listWorkflowExecutionsRequest);
    } else if (searchType.equals(_scanWorkflow)) {
      response = serviceOld.ScanWorkflowExecutions(listWorkflowExecutionsRequest);
    }
    return response;
  }

  private ListWorkflowExecutionsResponse appendResultsFromOldCluster(
      ListWorkflowExecutionsRequest listWorkflowExecutionsRequest,
      ListWorkflowExecutionsResponse response,
      String searchType)
      throws TException {
    int responsePageSize = response.getExecutions().size();
    int neededPageSize = listWorkflowExecutionsRequest.getPageSize() - responsePageSize;

    ListWorkflowExecutionsResponse fromResponse =
        callOldCluster(listWorkflowExecutionsRequest, neededPageSize, searchType);

    // if old cluster is empty
    if (fromResponse == null) {
      return response;
    }

    fromResponse.getExecutions().addAll(response.getExecutions());
    return fromResponse;
  }

  public boolean hasPrefix(byte[] s, byte[] prefix) {
    return s == null
        ? false
        : s.length >= prefix.length
            && Arrays.equals(Arrays.copyOfRange(s, 0, prefix.length), prefix);
  }

  /**
   * This method handles pagination and combines results from both the new and old workflow service
   * clusters. The method first checks if the nextPageToken is not set or starts with the marker
   * (_marker) to determine if it should query the new cluster (serviceNew) or combine results from
   * both the new and old clusters. If nextPageToken is set and doesn't start with the marker, it
   * queries the old cluster (serviceOld). In case the response from the new cluster is null, it
   * retries the request on the old cluster. If the number of workflow executions returned by the
   * new cluster is less than the pageSize, it appends results from the old cluster to the response.
   *
   * @param listRequest The ListWorkflowExecutionsRequest containing the query parameters, including
   *     domain, nextPageToken, pageSize, and other filtering options.
   * @return The ListWorkflowExecutionsResponse containing a list of WorkflowExecutionInfo
   *     representing the workflow executions that match the query criteria. The response also
   *     includes a nextPageToken to support pagination.
   * @throws TException if there's any communication error with the underlying workflow service.
   * @throws BadRequestError if the provided ListWorkflowExecutionsRequest is invalid (null or lacks
   *     a domain).
   */
  @Override
  public ListWorkflowExecutionsResponse ListWorkflowExecutions(
      ListWorkflowExecutionsRequest listRequest) throws TException {

    if (listRequest == null) {
      throw new BadRequestError("List request is null");
    } else if (Strings.isNullOrEmpty(listRequest.getDomain())) {
      throw new BadRequestError("Domain is null or empty");
    }
    if (!listRequest.isSetPageSize()) {
      listRequest.pageSize = _defaultPageSize;
    }

    if (!listRequest.isSetNextPageToken()
        || listRequest.getNextPageToken().length == 0
        || hasPrefix(listRequest.getNextPageToken(), _marker)) {
      if (hasPrefix(listRequest.getNextPageToken(), _marker) == true) {
        listRequest.setNextPageToken(
            Arrays.copyOfRange(
                listRequest.getNextPageToken(),
                _marker.length,
                listRequest.getNextPageToken().length));
      }
      ListWorkflowExecutionsResponse response = serviceNew.ListWorkflowExecutions(listRequest);
      if (response == null) return callOldCluster(listRequest, 0, _listWorkflow);

      if (response.getExecutions().size() < listRequest.getPageSize()) {
        return appendResultsFromOldCluster(listRequest, response, _listWorkflow);
      }

      byte[] combinedNextPageToken = new byte[_marker.length + response.getNextPageToken().length];
      System.arraycopy(_marker, 0, combinedNextPageToken, 0, _marker.length);
      System.arraycopy(
          response.getNextPageToken(),
          0,
          combinedNextPageToken,
          _marker.length,
          response.getNextPageToken().length);
      response.setNextPageToken(combinedNextPageToken);
      return response;
    }
    return callOldCluster(listRequest, 0, _listWorkflow);
  }

  /**
   * Scans workflow executions based on the provided request parameters, handling pagination and
   * combining results from the new and old clusters. The method queries the new cluster
   * (serviceNew) if nextPageToken is not set or starts with the marker (_marker). Otherwise, it
   * queries the old cluster (serviceOld). Results from the old cluster are appended if needed to
   * maintain correct pagination.
   *
   * @param listRequest The ListWorkflowExecutionsRequest containing query parameters.
   * @return The ListWorkflowExecutionsResponse with WorkflowExecutionInfo and nextPageToken.
   * @throws TException if there's any communication error with the workflow service.
   * @throws BadRequestError if the provided ListWorkflowExecutionsRequest is invalid.
   */
  @Override
  public ListWorkflowExecutionsResponse ScanWorkflowExecutions(
      ListWorkflowExecutionsRequest listRequest) throws TException {
    ListWorkflowExecutionsResponse response;
    if (listRequest == null) {
      throw new BadRequestError("List request is null");
    } else if (Strings.isNullOrEmpty(listRequest.getDomain())) {
      throw new BadRequestError("Domain is null or empty");
    }
    if (!listRequest.isSetPageSize()) {
      listRequest.pageSize = _defaultPageSize;
    }

    if (!listRequest.isSetNextPageToken()
        || listRequest.getNextPageToken().length == 0
        || hasPrefix(listRequest.getNextPageToken(), _marker)) {
      if (hasPrefix(listRequest.getNextPageToken(), _marker)) {
        listRequest.setNextPageToken(
            Arrays.copyOfRange(
                listRequest.getNextPageToken(),
                _marker.length,
                listRequest.getNextPageToken().length));
      }
      response = serviceNew.ScanWorkflowExecutions(listRequest);
      if (response == null) return callOldCluster(listRequest, 0, _scanWorkflow);

      if (response.getExecutions().size() < listRequest.getPageSize()) {
        return appendResultsFromOldCluster(listRequest, response, _scanWorkflow);
      }

      byte[] combinedNextPageToken = new byte[_marker.length + response.getNextPageToken().length];
      System.arraycopy(_marker, 0, combinedNextPageToken, 0, _marker.length);
      System.arraycopy(
          response.getNextPageToken(),
          0,
          combinedNextPageToken,
          _marker.length,
          response.getNextPageToken().length);
      response.setNextPageToken(combinedNextPageToken);
      return response;
    }
    return callOldCluster(listRequest, 0, _scanWorkflow);
  }

  @Override
  public ListOpenWorkflowExecutionsResponse ListOpenWorkflowExecutions(
      ListOpenWorkflowExecutionsRequest listRequest) throws TException {
    ListOpenWorkflowExecutionsResponse response;
    if (listRequest == null) {
      throw new BadRequestError("List request is null");
    } else if (Strings.isNullOrEmpty(listRequest.getDomain())) {
      throw new BadRequestError("Domain is null or empty");
    }
    if (!listRequest.isSetMaximumPageSize()) {
      listRequest.maximumPageSize = _defaultPageSize;
    }

    if (!listRequest.isSetNextPageToken()
        || listRequest.getNextPageToken().length == 0
        || hasPrefix(listRequest.getNextPageToken(), _marker)) {
      if (hasPrefix(listRequest.getNextPageToken(), _marker)) {
        listRequest.setNextPageToken(
            Arrays.copyOfRange(
                listRequest.getNextPageToken(),
                _marker.length,
                listRequest.getNextPageToken().length));
      }
      response = serviceNew.ListOpenWorkflowExecutions(listRequest);
      if (response == null) return serviceOld.ListOpenWorkflowExecutions(listRequest);

      if (response.getExecutionsSize() < listRequest.getMaximumPageSize()) {
        int neededPageSize = listRequest.getMaximumPageSize() - response.getExecutionsSize();
        ListOpenWorkflowExecutionsRequest copiedRequest =
            new ListOpenWorkflowExecutionsRequest(listRequest);
        copiedRequest.maximumPageSize = neededPageSize;
        ListOpenWorkflowExecutionsResponse fromResponse =
            serviceOld.ListOpenWorkflowExecutions(copiedRequest);
        if (fromResponse == null) return response;

        fromResponse.getExecutions().addAll(response.getExecutions());
        return fromResponse;
      }

      byte[] combinedNextPageToken = new byte[_marker.length + response.getNextPageToken().length];
      System.arraycopy(_marker, 0, combinedNextPageToken, 0, _marker.length);
      System.arraycopy(
          response.getNextPageToken(),
          0,
          combinedNextPageToken,
          _marker.length,
          response.getNextPageToken().length);
      response.setNextPageToken(combinedNextPageToken);
      return response;
    }
    return serviceOld.ListOpenWorkflowExecutions(listRequest);
  }

  @Override
  public ListClosedWorkflowExecutionsResponse ListClosedWorkflowExecutions(
      ListClosedWorkflowExecutionsRequest listRequest) throws TException {
    ListClosedWorkflowExecutionsResponse response;
    if (listRequest == null) {
      throw new BadRequestError("List request is null");
    } else if (Strings.isNullOrEmpty(listRequest.getDomain())) {
      throw new BadRequestError("Domain is null or empty");
    }
    if (!listRequest.isSetMaximumPageSize()) {
      listRequest.maximumPageSize = _defaultPageSize;
    }

    if (!listRequest.isSetNextPageToken()
        || listRequest.getNextPageToken().length == 0
        || hasPrefix(listRequest.getNextPageToken(), _marker)) {
      if (hasPrefix(listRequest.getNextPageToken(), _marker)) {
        listRequest.setNextPageToken(
            Arrays.copyOfRange(
                listRequest.getNextPageToken(),
                _marker.length,
                listRequest.getNextPageToken().length));
      }
      response = serviceNew.ListClosedWorkflowExecutions(listRequest);
      if (response == null) return serviceOld.ListClosedWorkflowExecutions(listRequest);

      if (response.getExecutionsSize() < listRequest.getMaximumPageSize()) {
        int neededPageSize = listRequest.getMaximumPageSize() - response.getExecutionsSize();
        ListClosedWorkflowExecutionsRequest copiedRequest =
            new ListClosedWorkflowExecutionsRequest(listRequest);
        copiedRequest.maximumPageSize = neededPageSize;
        ListClosedWorkflowExecutionsResponse fromResponse =
            serviceOld.ListClosedWorkflowExecutions(copiedRequest);
        if (fromResponse == null) return response;

        fromResponse.getExecutions().addAll(response.getExecutions());
        return fromResponse;
      }

      byte[] combinedNextPageToken = new byte[_marker.length + response.getNextPageToken().length];
      System.arraycopy(_marker, 0, combinedNextPageToken, 0, _marker.length);
      System.arraycopy(
          response.getNextPageToken(),
          0,
          combinedNextPageToken,
          _marker.length,
          response.getNextPageToken().length);
      response.setNextPageToken(combinedNextPageToken);
      return response;
    }
    return serviceOld.ListClosedWorkflowExecutions(listRequest);
  }

  @Override
  public QueryWorkflowResponse QueryWorkflow(QueryWorkflowRequest queryRequest) throws TException {

    try {
      if (shouldStartInNew(queryRequest.getExecution().getWorkflowId()))
        return serviceNew.QueryWorkflow(queryRequest);
      return serviceOld.QueryWorkflow(queryRequest);
    } catch (NullPointerException e) {
      throw new NullPointerException(
          "Query does not have workflowID associated: " + e.getMessage());
    }
  }

  @Override
  public CountWorkflowExecutionsResponse CountWorkflowExecutions(
      CountWorkflowExecutionsRequest countRequest) throws TException {

    CountWorkflowExecutionsResponse countResponseNew =
        serviceNew.CountWorkflowExecutions(countRequest);
    CountWorkflowExecutionsResponse countResponseOld =
        serviceOld.CountWorkflowExecutions(countRequest);
    if (countResponseNew == null) return countResponseOld;
    if (countResponseOld == null) return countResponseNew;

    countResponseOld.setCount(countResponseOld.getCount() + countResponseNew.getCount());
    return countResponseOld;
  }

  @Override
  public void TerminateWorkflowExecution(TerminateWorkflowExecutionRequest terminateRequest)
      throws TException {
    try {
      serviceNew.TerminateWorkflowExecution(terminateRequest);
    } catch (EntityNotExistsError e) {
      serviceOld.TerminateWorkflowExecution(terminateRequest);
    }
  }

  private Boolean shouldStartInNew(String workflowID) throws TException {
    try {
      return describeWorkflowExecution(serviceNew, domainNew, workflowID)
          .thenCombine(
              describeWorkflowExecution(serviceOld, domainOld, workflowID),
              (respNew, respOld) ->
                  respNew != null // execution already in new
                      || respOld == null // execution not exist in new and not exist in old
                      || (respOld.isSetWorkflowExecutionInfo()
                          && respOld
                              .getWorkflowExecutionInfo()
                              .isSetCloseStatus()) // execution not exist in new and execution is
              // closed in old
              )
          .get();
    } catch (CompletionException e) {
      throw e.getCause() instanceof TException
          ? (TException) e.getCause()
          : new TException("unknown error: " + e.getMessage());
    } catch (Exception e) {
      throw new TException("Unknown error: " + e.getMessage());
    }
  }

  private CompletableFuture<DescribeWorkflowExecutionResponse> describeWorkflowExecution(
      IWorkflowService service, String domain, String workflowID) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return service.DescribeWorkflowExecution(
                new DescribeWorkflowExecutionRequest()
                    .setDomain(domain)
                    .setExecution(new WorkflowExecution().setWorkflowId(workflowID)));
          } catch (EntityNotExistsError e) {
            return null;
          } catch (Exception e) {
            throw new CompletionException(e);
          }
        });
  }
}
