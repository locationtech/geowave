package mil.nga.giat.geowave.datastore.cassandra.metadata;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

public class CassandraIndexStore extends
		AbstractCassandraPersistence<Index<?, ?>> implements
		IndexStore
{

	private final static Logger LOGGER = Logger.getLogger(
			CassandraIndexStore.class);
	protected static final String INDEX_CF = "INDEX";

	public CassandraIndexStore(
			final CassandraOperations operations ) {
		super(
				operations);
	}

	@Override
	public void addIndex(
			final Index index ) {
		addObject(
				index);

	}

	@Override
	public Index getIndex(
			final ByteArrayId indexId ) {
		return getObject(
				indexId,
				null);
	}

	@Override
	public boolean indexExists(
			final ByteArrayId id ) {
		return objectExists(
				id,
				null);
	}

	@Override
	public CloseableIterator<Index<?, ?>> getIndices() {
		return getObjects();
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final Index persistedObject ) {
		return persistedObject.getId();
	}

	@Override
	protected String getPersistenceTypeName() {
		return INDEX_CF;
	}

}
