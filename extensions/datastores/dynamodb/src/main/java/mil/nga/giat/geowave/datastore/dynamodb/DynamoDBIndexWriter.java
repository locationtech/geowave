package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.DataStoreIndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class DynamoDBIndexWriter<T> extends
		DataStoreIndexWriter<T, WriteRequest>
{
	public static final Integer PARTITIONS = 32;
	protected final AmazonDynamoDBAsyncClient client;
	protected final DynamoDBOperations dynamodbOperations;
	private static long counter = 0;

	public DynamoDBIndexWriter(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final DynamoDBOperations operations,
			final IngestCallback<T> callback,
			final Closeable closable ) {
		super(
				adapter,
				index,
				null,
				null,
				callback,
				closable);
		this.client = operations.getClient();
		this.dynamodbOperations = operations;
	}

	@Override
	protected void ensureOpen() {
		if (writer == null) {
			writer = dynamodbOperations.createWriter(
					StringUtils.stringFromBinary(index.getId().getBytes()),
					true);
		}
	}

	private static <T> List<WriteRequest> getWriteRequests(
			final byte[] adapterId,
			final DataStoreEntryInfo ingestInfo ) {
		final List<WriteRequest> mutations = new ArrayList<WriteRequest>();
		final List<byte[]> fieldInfoBytesList = new ArrayList<>();
		int totalLength = 0;
		for (final FieldInfo<?> fieldInfo : ingestInfo.getFieldInfo()) {
			final ByteBuffer fieldInfoBytes = ByteBuffer.allocate(4 + fieldInfo.getWrittenValue().length);
			fieldInfoBytes.putInt(fieldInfo.getWrittenValue().length);
			fieldInfoBytes.put(fieldInfo.getWrittenValue());
			fieldInfoBytesList.add(fieldInfoBytes.array());
			totalLength += fieldInfoBytes.array().length;
		}
		final ByteBuffer allFields = ByteBuffer.allocate(totalLength);
		for (final byte[] bytes : fieldInfoBytesList) {
			allFields.put(bytes);
		}
		for (final ByteArrayId insertionId : ingestInfo.getInsertionIds()) {
			final Map<String, AttributeValue> map = new HashMap<String, AttributeValue>();
			final ByteBuffer idBuffer = ByteBuffer.allocate(ingestInfo.getDataId().length + adapterId.length + 4);
			idBuffer.putInt(ingestInfo.getDataId().length);
			idBuffer.put(ingestInfo.getDataId());
			idBuffer.put(adapterId);
			idBuffer.rewind();
			map.put(
					DynamoDBRow.GW_ID_KEY,
					new AttributeValue().withB(idBuffer));
			map.put(
					DynamoDBRow.GW_PARTITION_ID_KEY,
					new AttributeValue().withN(Long.toString(counter++ % PARTITIONS)));
			map.put(
					DynamoDBRow.GW_IDX_KEY,
					new AttributeValue().withB(ByteBuffer.wrap(insertionId.getBytes())));
			allFields.rewind();
			map.put(
					DynamoDBRow.GW_VALUE_KEY,
					new AttributeValue().withB(allFields));
			mutations.add(new WriteRequest(
					new PutRequest(
							map)));
		}
		return mutations;
	}

	@Override
	protected DataStoreEntryInfo getEntryInfo(
			final T entry,
			final VisibilityWriter<T> visibilityWriter ) {
		final DataStoreEntryInfo entryInfo = DataStoreUtils.getIngestInfo(
				(WritableDataAdapter<T>) adapter,
				index,
				entry,
				DataStoreUtils.UNCONSTRAINED_VISIBILITY);
		if (entryInfo != null) {
			writer.write(getWriteRequests(
					adapterId,
					entryInfo));
		}
		return entryInfo;
	}

}
