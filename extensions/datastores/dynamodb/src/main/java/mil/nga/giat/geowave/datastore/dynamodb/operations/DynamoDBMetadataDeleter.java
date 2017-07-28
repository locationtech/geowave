package mil.nga.giat.geowave.datastore.dynamodb.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;

import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.core.store.operations.MetadataDeleter;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataType;

public class DynamoDBMetadataDeleter implements
		MetadataDeleter
{
	private final static Logger LOGGER = Logger.getLogger(DynamoDBMetadataDeleter.class);

	private final DynamoDBOperations operations;
	private final String metadataTypeName;

	public DynamoDBMetadataDeleter(
			final DynamoDBOperations operations,
			final MetadataType metadataType ) {
		super();
		this.operations = operations;
		metadataTypeName = metadataType.name();
	}

	@Override
	public void close()
			throws Exception {}

	@Override
	public boolean delete(
			final MetadataQuery metadata ) {
		// the nature of metadata deleter is that primary ID is always
		// well-defined and it is deleting a single entry at a time
		String tableName = operations.getQualifiedTableName(AbstractGeoWavePersistence.METADATA_TABLE);

		final Map<String, AttributeValue> key = new HashMap<>();
		key.put(
				DynamoDBOperations.METADATA_PRIMARY_ID_KEY,
				new AttributeValue().withB(ByteBuffer.wrap(metadata.getPrimaryId())));

		if (metadata.getSecondaryId() != null) {
			key.put(
					DynamoDBOperations.METADATA_SECONDARY_ID_KEY,
					new AttributeValue().withB(ByteBuffer.wrap(metadata.getSecondaryId())));
		}

		operations.getClient().deleteItem(
				tableName,
				key);

		return true;
	}

	@Override
	public void flush() {}

}
