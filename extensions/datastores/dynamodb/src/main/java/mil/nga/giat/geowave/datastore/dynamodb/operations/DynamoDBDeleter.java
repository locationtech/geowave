package mil.nga.giat.geowave.datastore.dynamodb.operations;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;

public class DynamoDBDeleter implements
		Deleter
{
	private static Logger LOGGER = LoggerFactory.getLogger(DynamoDBDeleter.class);

	private final DynamoDBOperations operations;
	private final String tableName;

	public DynamoDBDeleter(
			final DynamoDBOperations operations,
			final String qualifiedTableName ) {
		this.operations = operations;
		tableName = qualifiedTableName;
	}

	@Override
	public void close()
			throws Exception {}

	@Override
	public void delete(
			final GeoWaveRow row,
			final DataAdapter<?> adapter ) {
		final DynamoDBRow dynRow = (DynamoDBRow) row;

		for (final Map<String, AttributeValue> attributeMappings : dynRow.getAttributeMapping()) {
			operations.getClient().deleteItem(
					tableName,
					Maps.filterEntries(
							attributeMappings,
							new Predicate<Entry<String, AttributeValue>>() {
								@Override
								public boolean apply(
										final Entry<String, AttributeValue> input ) {
									return DynamoDBRow.GW_PARTITION_ID_KEY.equals(input.getKey())
											|| DynamoDBRow.GW_RANGE_KEY.equals(input.getKey());
								}
							}));
		}
	}

}
