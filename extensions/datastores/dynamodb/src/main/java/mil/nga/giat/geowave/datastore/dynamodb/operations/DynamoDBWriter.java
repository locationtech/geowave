package mil.nga.giat.geowave.datastore.dynamodb.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;

public class DynamoDBWriter implements
		Writer
{
	private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBWriter.class);
	private static final int NUM_ITEMS = 25;
	private static final boolean ASYNC_WRITE = false;
	// because DynamoDB requires a hash key, if the geowave partition key is
	// empty, we need a non-empty constant alternative
	protected static final byte[] EMPTY_PARTITION_KEY = new byte[] {
		0
	};
	private final List<WriteRequest> batchedItems = new ArrayList<>();
	private final String tableName;
	private final AmazonDynamoDBAsyncClient client;
	private final Map<AmazonWebServiceRequest, Future> futureMap = new Hashtable<>();

	public DynamoDBWriter(
			final AmazonDynamoDBAsyncClient client,
			final String tableName ) {
		this.client = client;
		this.tableName = tableName;
	}

	@Override
	public void close()
			throws IOException {
		flush();
	}

	@Override
	public void write(
			final GeoWaveRow[] rows ) {
		final List<WriteRequest> mutations = new ArrayList<WriteRequest>();

		for (final GeoWaveRow row : rows) {
			mutations.addAll(rowToMutations(row));
		}

		write(mutations);
	}

	@Override
	public void write(
			final GeoWaveRow row ) {
		write(rowToMutations(row));
	}

	public void write(
			final Iterable<WriteRequest> items ) {
		for (final WriteRequest item : items) {
			write(item);
		}
	}

	public void write(
			final WriteRequest item ) {
		synchronized (batchedItems) {
			batchedItems.add(item);
			if (batchedItems.size() >= NUM_ITEMS) {
				do {
					writeBatch(ASYNC_WRITE);
				}
				while (batchedItems.size() >= NUM_ITEMS);
			}
		}
	}

	private void writeBatch(
			final boolean async ) {
		final List<WriteRequest> batch;

		if (batchedItems.size() <= NUM_ITEMS) {
			batch = batchedItems;
		}
		else {
			batch = batchedItems.subList(
					0,
					NUM_ITEMS + 1);
		}
		final Map<String, List<WriteRequest>> writes = new HashMap<>();
		writes.put(
				tableName,
				new ArrayList<>(
						batch));
		if (async) {

			/**
			 * To support asynchronous batch write a async handler is created
			 * Callbacks are provided for success and error. As there might be
			 * unprocessed items on failure, they are retried asynchronously
			 * Keep track of futures, so that they can be waited on during
			 * "flush"
			 */
			final BatchWriteItemRequest batchRequest = new BatchWriteItemRequest(
					writes);
			final Future<BatchWriteItemResult> future = client.batchWriteItemAsync(
					batchRequest,
					new AsyncHandler<BatchWriteItemRequest, BatchWriteItemResult>() {

						@Override
						public void onError(
								final Exception exception ) {
							LOGGER.warn("Unable to get response from Dynamo-Async Write " + exception.toString());
							futureMap.remove(batchRequest);
							return;
						}

						@Override
						public void onSuccess(
								final BatchWriteItemRequest request,
								final BatchWriteItemResult result ) {
							retryAsync(result.getUnprocessedItems());
							if (futureMap.remove(request) == null) {
								LOGGER.warn(" Unable to delete BatchWriteRequest from futuresMap ");
							}
						}

					});

			futureMap.put(
					batchRequest,
					future);
		}
		else {
			final BatchWriteItemResult response = client.batchWriteItem(new BatchWriteItemRequest(
					writes));
			retry(response.getUnprocessedItems());
		}

		batch.clear();
	}

	private void retry(
			final Map<String, List<WriteRequest>> map ) {
		for (final Entry<String, List<WriteRequest>> requests : map.entrySet()) {
			for (final WriteRequest r : requests.getValue()) {
				if (r.getPutRequest() != null) {
					client.putItem(
							requests.getKey(),
							r.getPutRequest().getItem());
				}
			}
		}
	}

	private void retryAsync(
			final Map<String, List<WriteRequest>> map ) {
		for (final Entry<String, List<WriteRequest>> requests : map.entrySet()) {
			for (final WriteRequest r : requests.getValue()) {
				if (r.getPutRequest() != null) {

					/**
					 * The code is pretty similar to retry. The only difference
					 * is retryAsync uses putItemAsync instead of putItem
					 */
					final PutItemRequest putRequest = new PutItemRequest(
							requests.getKey(),
							r.getPutRequest().getItem());
					final Future<PutItemResult> future = client.putItemAsync(
							putRequest,
							new AsyncHandler<PutItemRequest, PutItemResult>() {

								@Override
								public void onError(
										final Exception exception ) {
									LOGGER.warn("Putitem Async failed in Dynamo");
									futureMap.remove(putRequest);
								}

								@Override
								public void onSuccess(
										final PutItemRequest request,
										final PutItemResult result ) {
									if (futureMap.remove(request) == null) {
										LOGGER.warn("Unable to delete PutItemRequest from futuresMap ");
									}

									return;
								}

							});

					futureMap.put(
							putRequest,
							future);
				}
			}
		}
	}

	@Override
	public void flush() {
		synchronized (batchedItems) {
			while (!batchedItems.isEmpty()) {
				writeBatch(ASYNC_WRITE);
			}

			/**
			 * If its asynchronous, wait for future jobs to complete before we
			 * consider flush complete
			 */
			for (final Future future : futureMap.values()) {
				if (!future.isDone() && !future.isCancelled()) {
					try {
						future.get();
					}
					catch (final InterruptedException e) {
						LOGGER.error(
								"Future interrupted",
								e);
					}
					catch (final ExecutionException e) {
						LOGGER.error(
								"Execution exception ",
								e);
					}
				}
			}
		}
	}

	private static List<WriteRequest> rowToMutations(
			final GeoWaveRow row ) {
		final ArrayList<WriteRequest> mutations = new ArrayList<>();

		final byte[] rowId = DynamoDBRow.getRangeKey(row);

		final Map<String, AttributeValue> map = new HashMap<String, AttributeValue>();

		byte[] partitionKey = row.getPartitionKey();
		if ((partitionKey == null) || (partitionKey.length == 0)) {
			partitionKey = EMPTY_PARTITION_KEY;
		}

		for (final GeoWaveValue value : row.getFieldValues()) {

			map.put(
					DynamoDBRow.GW_PARTITION_ID_KEY,
					new AttributeValue().withB(ByteBuffer.wrap(partitionKey)));

			map.put(
					DynamoDBRow.GW_RANGE_KEY,
					new AttributeValue().withB(ByteBuffer.wrap(rowId)));

			map.put(
					DynamoDBRow.GW_FIELD_MASK_KEY,
					new AttributeValue().withB(ByteBuffer.wrap(value.getFieldMask())));

			map.put(
					DynamoDBRow.GW_VALUE_KEY,
					new AttributeValue().withB(ByteBuffer.wrap(value.getValue())));

			mutations.add(new WriteRequest(
					new PutRequest(
							map)));

		}

		return mutations;
	}
}
