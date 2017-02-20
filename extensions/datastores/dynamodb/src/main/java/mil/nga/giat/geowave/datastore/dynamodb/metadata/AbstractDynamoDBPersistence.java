package mil.nga.giat.geowave.datastore.dynamodb.metadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;

public abstract class AbstractDynamoDBPersistence<T extends Persistable> extends
		AbstractGeoWavePersistence<T>
{
	private final static Logger LOGGER = Logger.getLogger(AbstractDynamoDBPersistence.class);
	private static final String PRIMARY_ID_KEY = "I";
	private static final String SECONDARY_ID_KEY = "S";
	private static final String TIMESTAMP_KEY = "T";
	private static final String VALUE_KEY = "V";

	private final AmazonDynamoDBAsyncClient client;
	private final DynamoDBOperations dynamodbOperations;

	public AbstractDynamoDBPersistence(
			final DynamoDBOperations ops ) {
		super(
				ops);
		this.client = ops.getClient();
		dynamodbOperations = ops;
	}

	protected ByteArrayId getSecondaryId(
			final Map<String, AttributeValue> map ) {
		final AttributeValue v = map.get(SECONDARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.getB().array());
		}
		return null;
	}

	protected ByteArrayId getPrimaryId(
			final Map<String, AttributeValue> map ) {
		final AttributeValue v = map.get(PRIMARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.getB().array());
		}
		return null;
	}

	protected ByteArrayId getPersistenceTypeName(
			final Map<String, AttributeValue> map ) {
		final AttributeValue v = map.get(PRIMARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.getB().array());
		}
		return null;
	}

	@Override
	protected void addObject(
			final ByteArrayId id,
			final ByteArrayId secondaryId,
			final T object ) {
		addObjectToCache(
				id,
				secondaryId,
				object);

		final Map<String, AttributeValue> map = new HashMap<>();
		map.put(
				PRIMARY_ID_KEY,
				new AttributeValue().withB(ByteBuffer.wrap(id.getBytes())));
		if (secondaryId != null) {
			map.put(
					SECONDARY_ID_KEY,
					new AttributeValue().withB(ByteBuffer.wrap(secondaryId.getBytes())));
		}
		map.put(
				TIMESTAMP_KEY,
				new AttributeValue().withN(Long.toString(System.currentTimeMillis())));
		map.put(
				VALUE_KEY,
				new AttributeValue().withB(ByteBuffer.wrap(PersistenceUtils.toBinary(object))));

		final String tableName = dynamodbOperations.getQualifiedTableName(getTablename());
		synchronized (DynamoDBOperations.tableExistsCache) {
			final Boolean tableExists = DynamoDBOperations.tableExistsCache.get(tableName);
			if ((tableExists == null) || !tableExists) {
				final boolean tableCreated = TableUtils.createTableIfNotExists(
						client,
						new CreateTableRequest().withTableName(
								tableName).withAttributeDefinitions(
								new AttributeDefinition(
										PRIMARY_ID_KEY,
										ScalarAttributeType.B)).withKeySchema(
								new KeySchemaElement(
										PRIMARY_ID_KEY,
										KeyType.HASH)).withAttributeDefinitions(
								new AttributeDefinition(
										TIMESTAMP_KEY,
										ScalarAttributeType.N)).withKeySchema(
								new KeySchemaElement(
										TIMESTAMP_KEY,
										KeyType.RANGE)).withProvisionedThroughput(
								new ProvisionedThroughput(
										Long.valueOf(5),
										Long.valueOf(5))));
				if (tableCreated) {
					try {
						TableUtils.waitUntilActive(
								client,
								tableName);
					}
					catch (TableNeverTransitionedToStateException | InterruptedException e) {
						LOGGER.error(
								"Unable to wait for active table '" + tableName + "'",
								e);
					}
				}
				DynamoDBOperations.tableExistsCache.put(
						tableName,
						true);
			}
		}
		client.putItem(new PutItemRequest(
				tableName,
				map));
	}

	protected CloseableIterator<T> getAllObjectsWithSecondaryId(
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		final String tableName = dynamodbOperations.getQualifiedTableName(getTablename());
		final Boolean tableExists = DynamoDBOperations.tableExistsCache.get(tableName);
		if ((tableExists == null) || !tableExists) {
			try {
				if (!dynamodbOperations.tableExists(tableName)) {
					return new CloseableIterator.Wrapper<>(
							Iterators.emptyIterator());
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"unable to check table existence",
						e);
				return new CloseableIterator.Wrapper<>(
						Iterators.emptyIterator());
			}
		}
		final ScanResult result = client.scan(new ScanRequest(
				dynamodbOperations.getQualifiedTableName(getTablename())).addScanFilterEntry(
				SECONDARY_ID_KEY,
				new Condition().withAttributeValueList(
						new AttributeValue().withB(ByteBuffer.wrap(secondaryId.getBytes()))).withComparisonOperator(
						ComparisonOperator.EQ)));
		return new CloseableIterator.Wrapper<T>(
				getNativeIteratorWrapper(result.getItems().iterator()));
	}

	@Override
	protected String getTablename() {
		return getPersistenceTypeName() + "_" + super.getTablename();
	}

	@SuppressWarnings("unchecked")
	protected T getObject(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		final Object cacheResult = getObjectFromCache(
				primaryId,
				secondaryId);
		if (cacheResult != null) {
			return (T) cacheResult;
		}
		final List<Map<String, AttributeValue>> results = getResults(
				primaryId,
				secondaryId,
				authorizations);
		final Iterator<Map<String, AttributeValue>> it = results.iterator();
		if (!it.hasNext()) {
			LOGGER.warn("Object '" + getCombinedId(
					primaryId,
					secondaryId).getString() + "' not found");
			return null;
		}
		return getNativeIteratorWrapper(
				results.iterator()).next();
	}

	protected CloseableIterator<T> getObjects(
			final String... authorizations ) {
		final List<Map<String, AttributeValue>> results = getFullResults(authorizations);
		return new CloseableIterator.Wrapper<T>(
				getNativeIteratorWrapper(results.iterator()));
	}

	@SuppressWarnings("unchecked")
	protected T entryToValue(
			final Map<String, AttributeValue> entry ) {
		final T result = (T) PersistenceUtils.fromBinary(
				entry.get(
						VALUE_KEY).getB().array(),
				Persistable.class);
		if (result != null) {
			addObjectToCache(
					getPrimaryId(result),
					getSecondaryId(result),
					result);
		}
		return result;
	}

	private List<Map<String, AttributeValue>> getFullResults(
			final String... authorizations ) {
		return getResults(
				null,
				null,
				authorizations);
	}

	protected List<Map<String, AttributeValue>> getResults(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		final String tableName = dynamodbOperations.getQualifiedTableName(getTablename());
		final Boolean tableExists = DynamoDBOperations.tableExistsCache.get(tableName);
		if ((tableExists == null) || !tableExists) {
			try {
				if (!dynamodbOperations.tableExists(tableName)) {
					return Collections.EMPTY_LIST;
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"unable to check table existence",
						e);
				return Collections.EMPTY_LIST;
			}
		}
		if (primaryId != null) {
			final QueryRequest query = new QueryRequest(
					tableName);
			if (secondaryId != null) {
				query.addQueryFilterEntry(
						SECONDARY_ID_KEY,
						new Condition()
								.withAttributeValueList(
										new AttributeValue().withB(ByteBuffer.wrap(secondaryId.getBytes())))
								.withComparisonOperator(
										ComparisonOperator.EQ));
			}
			query.addKeyConditionsEntry(
					PRIMARY_ID_KEY,
					new Condition().withAttributeValueList(
							new AttributeValue().withB(ByteBuffer.wrap(primaryId.getBytes()))).withComparisonOperator(
							ComparisonOperator.EQ));
			final QueryResult result = client.query(query);
			return result.getItems();
		}

		final ScanRequest scan = new ScanRequest(
				tableName);
		// scan.addScanFilterEntry(
		// TYPE_KEY,
		// new Condition().withAttributeValueList(
		// new AttributeValue(
		// getPersistenceTypeName())).withComparisonOperator(ComparisonOperator.EQ));
		if (secondaryId != null) {
			scan.addScanFilterEntry(
					SECONDARY_ID_KEY,
					new Condition()
							.withAttributeValueList(
									new AttributeValue().withB(ByteBuffer.wrap(secondaryId.getBytes())))
							.withComparisonOperator(
									ComparisonOperator.EQ));
		}
		final ScanResult result = client.scan(scan);
		return result.getItems();
	}

	public boolean deleteObjects(
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		return deleteObjects(
				null,
				secondaryId,
				authorizations);
	}

	@Override
	public boolean deleteObjects(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		// TODO

		return true;
	}

	protected boolean objectExists(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		if (getObjectFromCache(
				primaryId,
				secondaryId) != null) {
			return true;
		}
		final List<Map<String, AttributeValue>> results = getResults(
				primaryId,
				secondaryId);

		final Iterator<Map<String, AttributeValue>> it = results.iterator();
		if (it.hasNext()) {
			// may as well cache the result
			return (entryToValue(it.next()) != null);
		}
		else {
			return false;
		}

	}

	protected Iterator<T> getNativeIteratorWrapper(
			final Iterator<Map<String, AttributeValue>> results ) {
		return new NativeIteratorWrapper(
				results);
	}

	private class NativeIteratorWrapper implements
			Iterator<T>
	{
		final private Iterator<Map<String, AttributeValue>> it;

		private NativeIteratorWrapper(
				final Iterator<Map<String, AttributeValue>> it ) {
			this.it = it;
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public T next() {
			final Map<String, AttributeValue> row = it.next();
			return entryToValue(row);
		}

		@Override
		public void remove() {
			it.remove();
		}

	}
}
