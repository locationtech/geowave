package mil.nga.giat.geowave.datastore.dynamodb.util;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
	private static final int MAX_ASYNC_QUERY_RESULTS = 1000;
	private static int totalAsyncRequestsInProgress = 0;

	private final AmazonDynamoDBAsyncClient dynamoDBClient;
	private final Lock lock = new ReentrantLock();
	private final Condition noResult = lock.newCondition();
	private Queue<QueryResult> asyncQueryResults;
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
		this.asyncQueryResults = new ArrayDeque<>(
				MAX_ASYNC_QUERY_RESULTS);
		this.asyncRequestsInProgress = 0;
		makeAsyncQuery();
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

		lock.lock();
		try {
			if (lastRequest == null && asyncQueryResults.isEmpty() == true) {
				return null;
			}

			QueryResult result = null;
			if (lastRequest != null && asyncRequestsInProgress == 0) {
				makeAsyncQuery();
			}

			while (asyncQueryResults.isEmpty() == true) {
				try {
					noResult.await();
				}
				catch (InterruptedException e) {
					LOGGER.error("Exception in Async paginated query " + e);
					e.printStackTrace();
				}
			}
			result = asyncQueryResults.remove();

			return result.getItems().iterator();
		}
		finally {
			lock.unlock();
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
		lock.lock();
		try {
			++asyncRequestsInProgress;
			dynamoDBClient.queryAsync(
					lastRequest,
					new AsyncHandler<QueryRequest, QueryResult>() {

						@Override
						public void onError(
								Exception exception ) {
							LOGGER.error(
									"Query async failed with Exception ",
									exception);
							try {
								lock.lock();
								--asyncRequestsInProgress;
								noResult.signal();
							}
							finally {
								lock.unlock();
							}

						}

						@Override
						public void onSuccess(
								QueryRequest request,
								QueryResult result ) {

							lock.lock();
							try {
								--asyncRequestsInProgress;
								synchronized (AsyncPaginatedQuery.class) {
									--totalAsyncRequestsInProgress;
								}

								if (result.getLastEvaluatedKey() != null && !result.getLastEvaluatedKey().isEmpty()) {
									lastRequest.setExclusiveStartKey(result.getLastEvaluatedKey());

									synchronized (AsyncPaginatedQuery.class) {
										if (totalAsyncRequestsInProgress < MAX_ASYNC_QUERY_RESULTS) {
											++totalAsyncRequestsInProgress;
											makeAsyncQuery();
										}
									}
								}
								else {
									lastRequest = null;
								}

								asyncQueryResults.add(result);
								noResult.signal();
							}
							finally {
								lock.unlock();
							}
						}
					});
		}
		finally {
			lock.unlock();
		}

		return;
	}

}