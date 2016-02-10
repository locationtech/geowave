/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase.metadata;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

/**
 * @author viggy Functionality similar to <code> AccumuloIndexStore </code>
 */
public class HBaseIndexStore extends
		AbstractHBasePersistence<Index<?, ?>> implements
		IndexStore
{

	private final static Logger LOGGER = Logger.getLogger(
			HBaseIndexStore.class);
	private static final String INDEX_CF = "INDEX";

	public HBaseIndexStore(
			final BasicHBaseOperations operations ) {
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
