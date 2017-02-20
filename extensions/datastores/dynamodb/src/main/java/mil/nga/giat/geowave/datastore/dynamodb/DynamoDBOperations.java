package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow.GuavaRowTranslationHelper;
import mil.nga.giat.geowave.datastore.dynamodb.util.LazyPaginatedScan;

public class DynamoDBOperations implements
		DataStoreOperations
{
	private final Logger LOGGER = LoggerFactory.getLogger(DynamoDBOperations.class);
	private final AmazonDynamoDBAsyncClient client;
	private final String gwNamespace;
	private final DynamoDBOptions options;
	public static Map<String, Boolean> tableExistsCache = new HashMap<>();

	public DynamoDBOperations(
			final DynamoDBOptions options ) {
		this.options = options;
		client = DynamoDBClientPool.getInstance().getClient(
				options);
		gwNamespace = options.getGeowaveNamespace();
	}

	public DynamoDBOptions getOptions() {
		return options;
	}

	public AmazonDynamoDBAsyncClient getClient() {
		return client;
	}

	public String getQualifiedTableName(
			final String tableName ) {
		return gwNamespace == null ? tableName : gwNamespace + "_" + tableName;
	}

	public Iterator<DynamoDBRow> getRows(
			final String tableName,
			final byte[][] dataIds,
			final byte[] adapterId,
			final String... additionalAuthorizations ) {
		final String qName = getQualifiedTableName(tableName);
		final ByteArrayId adapterIdObj = new ByteArrayId(
				adapterId);
		final Set<ByteArrayId> dataIdsSet = new HashSet<ByteArrayId>(
				dataIds.length);
		for (int i = 0; i < dataIds.length; i++) {
			dataIdsSet.add(new ByteArrayId(
					dataIds[i]));
		}
		final ScanRequest request = new ScanRequest(
				qName);
		final ScanResult scanResult = client.scan(request);
		final Iterator<DynamoDBRow> everything = Iterators.transform(
				new LazyPaginatedScan(
						scanResult,
						request,
						client),
				new GuavaRowTranslationHelper());
		return Iterators.filter(
				everything,
				new Predicate<DynamoDBRow>() {

					@Override
					public boolean apply(
							final DynamoDBRow input ) {
						return dataIdsSet.contains(new ByteArrayId(
								input.getDataId())) && new ByteArrayId(
								input.getAdapterId()).equals(adapterIdObj);
					}
				});
	}

	@Override
	public boolean tableExists(
			final String tableName )
			throws IOException {
		try {
			return TableStatus.ACTIVE.name().equals(
					client.describeTable(
							getQualifiedTableName(tableName)).getTable().getTableStatus());
		}
		catch (final AmazonDynamoDBException e) {
			LOGGER.info(
					"Unable to check existence of table",
					e);
		}
		return false;
	}

	@Override
	public void deleteAll()
			throws Exception {
		final ListTablesResult tables = client.listTables();
		for (final String tableName : tables.getTableNames()) {
			if ((gwNamespace == null) || tableName.startsWith(gwNamespace)) {
				client.deleteTable(new DeleteTableRequest(
						tableName));
			}
		}
		tableExistsCache.clear();
	}

	public boolean deleteRow(
			final String tableName,
			final DynamoDBRow row,
			final String... additionalAuthorizations ) {
		DeleteItemResult result = client.deleteItem(
				getQualifiedTableName(tableName),
				Maps.filterEntries(
						row.getAttributeMapping(),
						new Predicate<Entry<String, AttributeValue>>() {
							@Override
							public boolean apply(
									final Entry<String, AttributeValue> input ) {
								return DynamoDBRow.GW_PARTITION_ID_KEY.equals(input.getKey())
										|| DynamoDBRow.GW_RANGE_KEY.equals(input.getKey());
							}
						}));
		return result != null && result.getAttributes() != null && !result.getAttributes().isEmpty();
	}

	public Writer createWriter(
			final String tableName,
			final boolean createTable ) {
		final String qName = getQualifiedTableName(tableName);
		final DynamoDBWriter writer = new DynamoDBWriter(
				qName,
				client);
		if (createTable) {
			synchronized (tableExistsCache) {
				final Boolean tableExists = tableExistsCache.get(qName);
				if ((tableExists == null) || !tableExists) {
					final boolean tableCreated = TableUtils.createTableIfNotExists(
							client,
							new CreateTableRequest().withTableName(
									qName).withAttributeDefinitions(
									new AttributeDefinition(
											DynamoDBRow.GW_PARTITION_ID_KEY,
											ScalarAttributeType.N),
									new AttributeDefinition(
											DynamoDBRow.GW_RANGE_KEY,
											ScalarAttributeType.B)).withKeySchema(
									new KeySchemaElement(
											DynamoDBRow.GW_PARTITION_ID_KEY,
											KeyType.HASH),
									new KeySchemaElement(
											DynamoDBRow.GW_RANGE_KEY,
											KeyType.RANGE)).withProvisionedThroughput(
									new ProvisionedThroughput(
											Long.valueOf(options.getReadCapacity()),
											Long.valueOf(options.getWriteCapacity()))));
					if (tableCreated) {
						try {
							TableUtils.waitUntilActive(
									client,
									qName);
						}
						catch (TableNeverTransitionedToStateException | InterruptedException e) {
							LOGGER.error(
									"Unable to wait for active table '" + tableName + "'",
									e);
						}
					}
					tableExistsCache.put(
							qName,
							true);
				}
			}
		}
		return writer;
	}
}
