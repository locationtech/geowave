package mil.nga.giat.geowave.datastore.dynamodb.operations;

import java.nio.ByteBuffer;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.datastore.dynamodb.util.DynamoDBStatisticsIterator;
import mil.nga.giat.geowave.datastore.dynamodb.util.DynamoDBUtils;
import mil.nga.giat.geowave.datastore.dynamodb.util.DynamoDBUtils.NoopClosableIteratorWrapper;

public class DynamoDBMetadataReader implements
		MetadataReader
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DynamoDBMetadataReader.class);
	private final DynamoDBOperations operations;
	private final DataStoreOptions options;
	private final MetadataType metadataType;

	public DynamoDBMetadataReader(
			final DynamoDBOperations operations,
			final DataStoreOptions options,
			MetadataType metadataType ) {
		this.operations = operations;
		this.options = options;
		this.metadataType = metadataType;
	}

	@Override
	public CloseableIterator<GeoWaveMetadata> query(
			MetadataQuery query ) {
		String tableName = operations.getMetadataTableName(metadataType);

		if (query.hasPrimaryId()) {
			final QueryRequest queryRequest = new QueryRequest(
					tableName);

			if (query.hasSecondaryId()) {
				queryRequest.addQueryFilterEntry(
						DynamoDBOperations.METADATA_SECONDARY_ID_KEY,
						new Condition()
								.withAttributeValueList(
										new AttributeValue().withB(ByteBuffer.wrap(query.getSecondaryId())))
								.withComparisonOperator(
										ComparisonOperator.EQ));
			}
			queryRequest.addKeyConditionsEntry(
					DynamoDBOperations.METADATA_PRIMARY_ID_KEY,
					new Condition().withAttributeValueList(
							new AttributeValue().withB(ByteBuffer.wrap(query.getPrimaryId()))).withComparisonOperator(
							ComparisonOperator.EQ));

			final QueryResult queryResult = operations.getClient().query(
					queryRequest);

			if (metadataType == MetadataType.STATS) {
				return new DynamoDBStatisticsIterator(
						queryResult.getItems().iterator());
			}

			return new CloseableIteratorWrapper<>(
					new NoopClosableIteratorWrapper(),
					Iterators.transform(
							queryResult.getItems().iterator(),
							new com.google.common.base.Function<Map<String, AttributeValue>, GeoWaveMetadata>() {
								@Override
								public GeoWaveMetadata apply(
										final Map<String, AttributeValue> result ) {

									return new GeoWaveMetadata(
											DynamoDBUtils.getPrimaryId(result),
											DynamoDBUtils.getSecondaryId(result),
											null,
											DynamoDBUtils.getValue(result));
								}
							}));

		}

		final ScanRequest scan = new ScanRequest(
				tableName);
		if (query.hasSecondaryId()) {
			scan.addScanFilterEntry(
					DynamoDBOperations.METADATA_SECONDARY_ID_KEY,
					new Condition()
							.withAttributeValueList(
									new AttributeValue().withB(ByteBuffer.wrap(query.getSecondaryId())))
							.withComparisonOperator(
									ComparisonOperator.EQ));
		}
		final ScanResult scanResult = operations.getClient().scan(
				scan);

		if (metadataType == MetadataType.STATS) {
			return new DynamoDBStatisticsIterator(
					scanResult.getItems().iterator());
		}

		return new CloseableIteratorWrapper<>(
				new NoopClosableIteratorWrapper(),
				Iterators.transform(
						scanResult.getItems().iterator(),
						new com.google.common.base.Function<Map<String, AttributeValue>, GeoWaveMetadata>() {
							@Override
							public GeoWaveMetadata apply(
									final Map<String, AttributeValue> result ) {

								return new GeoWaveMetadata(
										DynamoDBUtils.getPrimaryId(result),
										DynamoDBUtils.getSecondaryId(result),
										null,
										DynamoDBUtils.getValue(result));
							}
						}));
	}

}
