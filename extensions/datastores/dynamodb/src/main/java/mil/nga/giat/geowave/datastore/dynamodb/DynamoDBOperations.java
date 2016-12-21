package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.base.Writer;

public class DynamoDBOperations implements
		DataStoreOperations
{
	private final Logger LOGGER = LoggerFactory.getLogger(DynamoDBOperations.class);
	private final AmazonDynamoDBAsyncClient client;
	private final String gwNamespace;
	private final DynamoDBOptions options;
	private static Map<String, Boolean> tableExistsCache = new HashMap<>();

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
	}

	public Writer createWriter(
			final String tableName,
			final boolean createTable ) {
		final String qName = getQualifiedTableName(tableName);
		final DynamoDBWriter writer = new DynamoDBWriter(
				qName,
				client);
		if (createTable) {
			final Boolean tableExists = tableExistsCache.get(qName);
			if ((tableExists == null) || !tableExists) {
				TableUtils.createTableIfNotExists(
						client,
						new CreateTableRequest().withTableName(
								qName).withAttributeDefinitions(
								new AttributeDefinition(
										DynamoDBRow.GW_PARTITION_ID_KEY,
										ScalarAttributeType.N),
								new AttributeDefinition(
										DynamoDBRow.GW_IDX_KEY,
										ScalarAttributeType.B)).withKeySchema(
								new KeySchemaElement(
										DynamoDBRow.GW_PARTITION_ID_KEY,
										KeyType.HASH),
								new KeySchemaElement(
										DynamoDBRow.GW_IDX_KEY,
										KeyType.RANGE)).withProvisionedThroughput(
								new ProvisionedThroughput(
										Long.valueOf(options.getReadCapacity()),
										Long.valueOf(options.getWriteCapacity()))));
				tableExistsCache.put(
						qName,
						true);
			}
		}
		return writer;
	}
}
