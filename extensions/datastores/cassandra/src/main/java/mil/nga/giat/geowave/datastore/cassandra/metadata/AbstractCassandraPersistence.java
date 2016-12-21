package mil.nga.giat.geowave.datastore.cassandra.metadata;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.datastax.driver.core.schemabuilder.Create;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeowavePersistence;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

abstract public class AbstractCassandraPersistence<T extends Persistable> extends
		AbstractGeowavePersistence<T>
{
	private static final String PRIMARY_ID_KEY = "I";
	private static final String SECONDARY_ID_KEY = "S";
	private static final String VALUE_KEY = "V";

	private final static Logger LOGGER = Logger.getLogger(
			AbstractCassandraPersistence.class);
	protected final CassandraOperations operations;
	
	public AbstractCassandraPersistence(
			final CassandraOperations operations ) {
		super(
				operations);
		this.operations = operations;
	}

	protected ByteArrayId getSecondaryId(
			final Row row ) {
		final ByteBuffer v = row.getBytes(
				SECONDARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.array());
		}
		return null;
	}

	protected ByteArrayId getPrimaryId(
			final Row row ) {
		final ByteBuffer v = row.getBytes(
				PRIMARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.array());
		}
		return null;
	}

	protected ByteArrayId getPersistenceTypeName(
			final Row row ) {
		final ByteBuffer v = row.getBytes(
				PRIMARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.array());
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
		if (!operations.tableExists(getTablename())) {
			// create table
			final Create create = operations.getCreateTable(
					getTablename());
			create.addPartitionKey(
					PRIMARY_ID_KEY,
					DataType.blob());
			create.addColumn(
					SECONDARY_ID_KEY,
					DataType.blob());
			create.addColumn(
					VALUE_KEY,
					DataType.blob());
		}
		final Insert insert = operations.getInsert(
				getTablename());
		insert.value(
				PRIMARY_ID_KEY,
				ByteBuffer.wrap(
						id.getBytes()));
		if (secondaryId != null) {
			insert.value(
					SECONDARY_ID_KEY,
					ByteBuffer.wrap(
							secondaryId.getBytes()));
		}
		insert.value(
				VALUE_KEY,
				ByteBuffer.wrap(
						PersistenceUtils.toBinary(
								object)));
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
				operations.getSelect(
						getTablename(),
						VALUE_KEY).where(
								QueryBuilder.eq(
										SECONDARY_ID_KEY,
										ByteBuffer.wrap(
												secondaryId.getBytes()))));
		return new CloseableIterator.Wrapper<T>(
				Iterators.transform(
						results.iterator(),
						new EntryToValueFunction()));
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
			LOGGER.warn(
					"Object '" + getCombinedId(
							primaryId,
							secondaryId).getString() + "' not found");
			return null;
		}
		final Row entry = results.next();
		return entryToValue(
				entry);
	}

	protected CloseableIterator<T> getObjects(
			final String... authorizations ) {
		final Iterator<Row> results = getFullResults(
				authorizations);
		return new CloseableIterator.Wrapper<T>(
				Iterators.transform(
						results,
						new EntryToValueFunction()));
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
					getPrimaryId(
							result),
					getSecondaryId(
							result),
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

	protected Iterator<Row> getResults(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		if (!operations.tableExists(getTablename())) {
			return Iterators.emptyIterator();
		}
		final Select select = operations.getSelect(
				getTablename(),
				VALUE_KEY);
		if (primaryId != null) {
			final Where where = select.where(
					QueryBuilder.eq(
							PRIMARY_ID_KEY,
							ByteBuffer.wrap(
									primaryId.getBytes())));
			if (secondaryId != null) {
				where.and(
						QueryBuilder.eq(
								SECONDARY_ID_KEY,
								ByteBuffer.wrap(
										secondaryId.getBytes())));
			}
		}
		return operations.getSession().execute(
				select).iterator();
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
			return (entryToValue(
					results.next()) != null);
		}
		else {
			return false;
		}

	}

	private class EntryToValueFunction implements
			Function<Row, T>
	{

		@Override
		public T apply(
				final Row entry ) {
			return entryToValue(
					entry);
		}

	}
}
