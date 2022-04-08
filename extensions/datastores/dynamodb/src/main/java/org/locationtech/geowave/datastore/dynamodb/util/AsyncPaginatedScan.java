/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.util;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import org.apache.commons.collections4.iterators.LazyIteratorChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class AsyncPaginatedScan extends LazyIteratorChain<Map<String, AttributeValue>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncPaginatedScan.class);
  private static final int MAX_ASYNC_SCAN_RESULTS = 100;
  private static int totalAsyncRequestsInProgress = 0;

  private final AmazonDynamoDBAsync dynamoDBClient;
  private final Object monitorLock = new Object();
  private final Deque<ScanResult> asyncScanResults;
  private ScanRequest lastRequest;
  private int asyncRequestsInProgress;

  /**
   * The async paginated query is a much more complicated but asynchronous version of the paginated
   * query
   *
   * <p> As soon a async paginated query is fired, multiple asynchronous query requests are fired in
   * tandem across different async paginated queries.
   *
   * <p> A max of "MAX_ASYNC_SCAN_RESULTS" can be in progress at any time
   */
  public AsyncPaginatedScan(final ScanRequest request, final AmazonDynamoDBAsync dynamoDBClient) {
    lastRequest = request;
    this.dynamoDBClient = dynamoDBClient;

    /**
     * Link list because we need to store null values Queues like ArrayDeque don't support null
     * value insertion
     */
    asyncScanResults = new LinkedList<>();
    asyncRequestsInProgress = 0;

    checkAndAsyncScan();
  }

  /**
   * Get the next query data If the last request is equal to null then we have no more query
   * requests to fire
   *
   * <p> If asyncQueryResults is not empty, we have already fetched the next query data that can be
   * read immediately
   *
   * <p> If due to max async query limit, we couldn't fire async requests, we fire the request now
   */
  @Override
  protected Iterator<? extends Map<String, AttributeValue>> nextIterator(final int arg0) {

    synchronized (monitorLock) {
      if ((lastRequest == null) && asyncScanResults.isEmpty()) {
        return null;
      }

      ScanResult result = null;
      if ((lastRequest != null) && (asyncRequestsInProgress == 0)) {
        makeAsyncScan();
      }

      while (asyncScanResults.isEmpty()) {
        try {
          monitorLock.wait();
        } catch (final InterruptedException e) {
          LOGGER.error("Exception in Async paginated query " + e);
          e.printStackTrace();
        }
      }
      result = asyncScanResults.remove();

      return result == null ? null : result.getItems().iterator();
    }
  }

  /**
   * Check if an async query should be fired and if necessary fire one Does not need the monitor
   * lock
   */
  private void checkAndAsyncScan() {
    synchronized (AsyncPaginatedQuery.class) {
      if (totalAsyncRequestsInProgress > MAX_ASYNC_SCAN_RESULTS) {
        return;
      }
      ++totalAsyncRequestsInProgress;
    }
    makeAsyncScan();
  }

  /** Reduce the number of total async requests in progress */
  private void decTotalAsyncRequestsInProgress() {
    synchronized (AsyncPaginatedQuery.class) {
      --totalAsyncRequestsInProgress;
    }
  }

  /**
   * Fire the async query On success, we check to see if we can fire any more queries We continue to
   * fire queries until the global max is reached or we have asynchronously fired all queries
   *
   * <p> Any waiting threads are signaled here
   */
  private void makeAsyncScan() {
    synchronized (monitorLock) {
      ++asyncRequestsInProgress;
      dynamoDBClient.scanAsync(lastRequest, new AsyncHandler<ScanRequest, ScanResult>() {

        /**
         * On Error, add a null and notify the thread waiting This makes sure that they are not
         * stuck waiting
         */
        @Override
        public void onError(final Exception exception) {
          LOGGER.error("Query async failed with Exception ", exception);
          synchronized (monitorLock) {
            --asyncRequestsInProgress;
            decTotalAsyncRequestsInProgress();
            asyncScanResults.add(null);
            monitorLock.notify();
          }
        }

        /**
         * On Success, fire a new request if we can Notify the waiting thread with the result
         */
        @Override
        public void onSuccess(final ScanRequest request, final ScanResult result) {

          synchronized (monitorLock) {
            --asyncRequestsInProgress;
            decTotalAsyncRequestsInProgress();

            if ((result.getLastEvaluatedKey() != null) && !result.getLastEvaluatedKey().isEmpty()) {
              lastRequest.setExclusiveStartKey(result.getLastEvaluatedKey());
              checkAndAsyncScan();
            } else {
              lastRequest = null;
            }

            asyncScanResults.add(result);
            monitorLock.notify();
          }
        }
      });
    }
    return;
  }
}
