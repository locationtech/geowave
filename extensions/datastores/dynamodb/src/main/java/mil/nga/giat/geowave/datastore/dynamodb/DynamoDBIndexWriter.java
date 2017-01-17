package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.Closeable;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.DataStoreIndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class DynamoDBIndexWriter<T> extends
		DataStoreIndexWriter<T, WriteRequest>
{
	protected final AmazonDynamoDBAsyncClient client;
	protected final DynamoDBDataStore dataStore;
	protected final DynamoDBOperations dynamodbOperations;

	public DynamoDBIndexWriter(
			final DynamoDBDataStore dataStore,
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
		this.dataStore = dataStore;
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

	@Override
	protected DataStoreEntryInfo getEntryInfo(
			final T entry,
			final VisibilityWriter<T> visibilityWriter ) {
		return dataStore.write(
				(WritableDataAdapter<T>) adapter,
				index,
				entry,
				writer,
				DataStoreUtils.UNCONSTRAINED_VISIBILITY);
	}
}
