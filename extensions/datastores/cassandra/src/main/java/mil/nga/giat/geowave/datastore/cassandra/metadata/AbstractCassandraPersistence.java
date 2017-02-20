package mil.nga.giat.geowave.datastore.cassandra.metadata;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.datastax.driver.core.schemabuilder.Create;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

abstract public class AbstractCassandraPersistence<T extends Persistable> extends
		AbstractGeoWavePersistence<T>
{
	protected static final String PRIMARY_ID_KEY = "I";
	protected static final String SECONDARY_ID_KEY = "S";
	// serves as unique ID for instances where primary+secondary are repeated
	protected static final String TIMESTAMP_ID_KEY = "T";
	protected static final String VALUE_KEY = "V";

	private static final Object CREATE_TABLE_MUTEX = new Object();

	private final static Logger LOGGER = Logger.getLogger(AbstractCassandraPersistence.class);
	protected final CassandraOperations operations;

	public AbstractCassandraPersistence(
			final CassandraOperations operations ) {
		super(
				operations);
		this.operations = operations;
	}

	protected ByteArrayId getSecondaryId(
			final Row row ) {
		final ByteBuffer v = row.getBytes(SECONDARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.array());
		}
		return null;
	}

	protected ByteArrayId getPersistenceTypeName(
			final Row row ) {
		final ByteBuffer v = row.getBytes(PRIMARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.array());
		}
		return null;
	}

	protected boolean hasSecondaryId() {
		return false;
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
		synchronized (CREATE_TABLE_MUTEX) {
			if (!operations.tableExists(getTablename())) {
				// create table
				final Create create = operations.getCreateTable(getTablename());
				create.addPartitionKey(
						PRIMARY_ID_KEY,
						DataType.blob());
				if (hasSecondaryId()) {
					create.addClusteringColumn(
							SECONDARY_ID_KEY,
							DataType.blob());
					create.addClusteringColumn(
							TIMESTAMP_ID_KEY,
							DataType.timeuuid());
				}
				create.addColumn(
						VALUE_KEY,
						DataType.blob());
				operations.executeCreateTable(
						create,
						getTablename());
			}
		}
		final Insert insert = operations.getInsert(getTablename());
		insert.value(
				PRIMARY_ID_KEY,
				ByteBuffer.wrap(id.getBytes()));
		if (secondaryId != null) {
			insert.value(
					SECONDARY_ID_KEY,
					ByteBuffer.wrap(secondaryId.getBytes()));
			insert.value(
					TIMESTAMP_ID_KEY,
					QueryBuilder.now());
		}
		insert.value(
				VALUE_KEY,
				ByteBuffer.wrap(PersistenceUtils.toBinary(object)));
		operations.getSession().execute(
				insert);
	}

	protected CloseableIterator<T> getAllObjectsWithSecondaryId(
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		if (!operations.tableExists(getTablename())) {
			return new CloseableIterator.Wrapper<>(
					Iterators.emptyIterator());
		}
		final ResultSet results = operations.getSession().execute(
				getSelect().allowFiltering().where(
						QueryBuilder.eq(
								SECONDARY_ID_KEY,
								ByteBuffer.wrap(secondaryId.getBytes()))));
		return new CloseableIterator.Wrapper<T>(
				getNativeIteratorWrapper(results.iterator()));
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
		final Iterator<Row> results = getResults(
				primaryId,
				secondaryId,
				authorizations);
		if (!results.hasNext()) {
			LOGGER.warn("Object '" + getCombinedId(
					primaryId,
					secondaryId).getString() + "' not found");
			return null;
		}
		return getNativeIteratorWrapper(
				results).next();
	}

	protected CloseableIterator<T> getObjects(
			final String... authorizations ) {
		final Iterator<Row> results = getFullResults(authorizations);
		return new CloseableIterator.Wrapper<T>(
				getNativeIteratorWrapper(results));
	}

	@SuppressWarnings("unchecked")
	protected T entryToValue(
			final Row entry ) {
		final T result = (T) PersistenceUtils.fromBinary(
				entry.get(
						VALUE_KEY,
						ByteBuffer.class).array(),
				Persistable.class);
		if (result != null) {
			addObjectToCache(
					getPrimaryId(result),
					getSecondaryId(result),
					result);
		}
		return result;
	}

	private Iterator<Row> getFullResults(
			final String... authorizations ) {
		return getResults(
				null,
				null,
				authorizations);
	}

	protected ByteArrayId getRowId(
			final T object ) {
		return getPrimaryId(object);
	}

	protected Iterator<Row> getResults(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		if (!operations.tableExists(getTablename())) {
			return Iterators.emptyIterator();
		}
		final Select select = getSelect();
		if (primaryId != null) {
			final Where where = select.where(QueryBuilder.eq(
					PRIMARY_ID_KEY,
					ByteBuffer.wrap(primaryId.getBytes())));
			if (secondaryId != null) {
				where.and(QueryBuilder.eq(
						SECONDARY_ID_KEY,
						ByteBuffer.wrap(secondaryId.getBytes())));
			}
		}
		return operations.getSession().execute(
				select).iterator();
	}

	protected Select getSelect() {
		return operations.getSelect(
				getTablename(),
				VALUE_KEY);
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
		final Iterator<Row> results = getResults(
				primaryId,
				secondaryId);

		if (results.hasNext()) {
			// may as well cache the result
			return (entryToValue(results.next()) != null);
		}
		else {
			return false;
		}

	}

	protected Iterator<T> getNativeIteratorWrapper(
			final Iterator<Row> results ) {
		return new NativeIteratorWrapper(
				results);
	}

	private class NativeIteratorWrapper implements
			Iterator<T>
	{
		final private Iterator<Row> it;

		private NativeIteratorWrapper(
				final Iterator<Row> it ) {
			this.it = it;
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public T next() {
			final Row row = it.next();
			return entryToValue(row);
		}

		@Override
		public void remove() {
			it.remove();
		}

	}
}
