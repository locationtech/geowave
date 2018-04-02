package mil.nga.giat.geowave.datastore.dynamodb.operations;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;

import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;

public class DynamoDBMetadataWriter implements
		MetadataWriter
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DynamoDBMetadataWriter.class);

	final DynamoDBOperations operations;
	private final String tableName;

	public DynamoDBMetadataWriter(
			final DynamoDBOperations operations,
			final String tableName ) {
		this.operations = operations;
		this.tableName = tableName;
	}

	@Override
	public void close()
			throws Exception {}

	@Override
	public void write(
			final GeoWaveMetadata metadata ) {
		final Map<String, AttributeValue> map = new HashMap<>();
		map.put(
				DynamoDBOperations.METADATA_PRIMARY_ID_KEY,
				new AttributeValue().withB(ByteBuffer.wrap(metadata.getPrimaryId())));

		if (metadata.getSecondaryId() != null) {
			map.put(
					DynamoDBOperations.METADATA_SECONDARY_ID_KEY,
					new AttributeValue().withB(ByteBuffer.wrap(metadata.getSecondaryId())));
		}

		map.put(
				DynamoDBOperations.METADATA_TIMESTAMP_KEY,
				new AttributeValue().withN(Long.toString(System.currentTimeMillis())));
		map.put(
				DynamoDBOperations.METADATA_VALUE_KEY,
				new AttributeValue().withB(ByteBuffer.wrap(metadata.getValue())));

		try {
			operations.getClient().putItem(
					new PutItemRequest(
							tableName,
							map));
		}
		catch (final Exception e) {
			LOGGER.error(
					"Error writing metadata",
					e);
		}
	}

	@Override
	public void flush() {}

}
