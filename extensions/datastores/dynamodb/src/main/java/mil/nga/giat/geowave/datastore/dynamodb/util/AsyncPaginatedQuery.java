package mil.nga.giat.geowave.datastore.dynamodb.util;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.collections4.iterators.LazyIteratorChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;

public class AsyncPaginatedQuery extends
		LazyIteratorChain<Map<String, AttributeValue>>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AsyncPaginatedQuery.class);
	private static final int MAX_ASYNC_QUERY_RESULTS = 100;
	private static int totalAsyncRequestsInProgress = 0;

	private final AmazonDynamoDBAsyncClient dynamoDBClient;
	private final Object monitorLock = new Object();
	private Deque<QueryResult> asyncQueryResults;
	private QueryRequest lastRequest;
	private int asyncRequestsInProgress;

	/**
	 * The async paginated query is a much more complicated but asynchronous
	 * version of the paginated query
	 * 
	 * As soon a async paginated query is fired, multiple asynchronous query
	 * requests are fired in tandem across different async paginated queries.
	 * 
	 * A max of "MAX_ASYNC_QUERY_RESULTS" can be in progress at any time
	 * 
	 */
	public AsyncPaginatedQuery(
			final QueryRequest request,
			final AmazonDynamoDBAsyncClient dynamoDBClient ) {
		this.lastRequest = request;
		this.dynamoDBClient = dynamoDBClient;

		/**
		 * Link list because we need to store null values Queues like ArrayDeque
		 * don't support null value insertion
		 */
		this.asyncQueryResults = new LinkedList<>();
		this.asyncRequestsInProgress = 0;

		checkAndAsyncQuery();
	}

	/**
	 * Get the next query data If the last request is equal to null then we have
	 * no more query requests to fire
	 * 
	 * If asyncQueryResults is not empty, we have already fetched the next query
	 * data that can be read immediately
	 * 
	 * If due to max async query limit, we couldn't fire async requests, we fire
	 * the request now
	 */
	protected Iterator<? extends Map<String, AttributeValue>> nextIterator(
			int arg0 ) {

		synchronized (monitorLock) {
			if (lastRequest == null && asyncQueryResults.isEmpty()) {
				return null;
			}

			QueryResult result = null;
			if (lastRequest != null && asyncRequestsInProgress == 0) {
				makeAsyncQuery();
			}

			while (asyncQueryResults.isEmpty()) {
				try {
					monitorLock.wait();
				}
				catch (InterruptedException e) {
					LOGGER.error("Exception in Async paginated query " + e);
					e.printStackTrace();
				}
			}
			result = asyncQueryResults.remove();

			return result == null ? null : result.getItems().iterator();
		}
	}

	/**
	 * Check if an async query should be fired and if necessary fire one Does
	 * not need the monitor lock
	 */
	private void checkAndAsyncQuery() {
		synchronized (AsyncPaginatedQuery.class) {
			if (totalAsyncRequestsInProgress > MAX_ASYNC_QUERY_RESULTS) {
				return;
			}
			++totalAsyncRequestsInProgress;
		}
		makeAsyncQuery();
	}

	/**
	 * Reduce the number of total async requests in progress
	 */
	private void decTotalAsyncRequestsInProgress() {
		synchronized (AsyncPaginatedQuery.class) {
			--totalAsyncRequestsInProgress;
		}
	}

	/**
	 * Fire the async query On success, we check to see if we can fire any more
	 * queries We continue to fire queries until the global max is reached or we
	 * have asynchronously fired all queries
	 * 
	 * Any waiting threads are signaled here
	 */
	private void makeAsyncQuery() {
		synchronized (monitorLock) {
			++asyncRequestsInProgress;
			dynamoDBClient.queryAsync(
					lastRequest,
					new AsyncHandler<QueryRequest, QueryResult>() {

						/**
						 * On Error, add a null and notify the thread waiting
						 * This makes sure that they are not stuck waiting
						 */
						@Override
						public void onError(
								Exception exception ) {
							LOGGER.error(
									"Query async failed with Exception ",
									exception);
							synchronized (monitorLock) {
								--asyncRequestsInProgress;
								decTotalAsyncRequestsInProgress();
								asyncQueryResults.add(null);
								monitorLock.notify();
							}

						}

						/**
						 * On Success, fire a new request if we can Notify the
						 * waiting thread with the result
						 */
						@Override
						public void onSuccess(
								QueryRequest request,
								QueryResult result ) {

							synchronized (monitorLock) {
								--asyncRequestsInProgress;
								decTotalAsyncRequestsInProgress();

								if (result.getLastEvaluatedKey() != null && !result.getLastEvaluatedKey().isEmpty()) {
									lastRequest.setExclusiveStartKey(result.getLastEvaluatedKey());
									checkAndAsyncQuery();
								}
								else {
									lastRequest = null;
								}

								asyncQueryResults.add(result);
								monitorLock.notify();
							}
						}
					});
		}
		return;
	}

}