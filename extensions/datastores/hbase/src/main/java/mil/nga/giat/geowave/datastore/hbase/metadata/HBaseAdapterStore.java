/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.metadata;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> AccumuloAdapterStore </code>
 */
public class HBaseAdapterStore extends
		AbstractHBasePersistence<DataAdapter<?>> implements
		AdapterStore
{
	private final static Logger LOGGER = Logger.getLogger(HBaseAdapterStore.class);
	private static final String ADAPTER_CF = "ADAPTER";

	public HBaseAdapterStore(
			BasicHBaseOperations operation ) {
		super(
				operation);
	}

	@Override
	public void addAdapter(
			DataAdapter<?> adapter ) {
		addObject(adapter);

	}

	@Override
	public DataAdapter<?> getAdapter(
			ByteArrayId adapterId ) {
		return getObject(
				adapterId,
				null);
	}

	@Override
	public boolean adapterExists(
			ByteArrayId adapterId ) {
		return objectExists(
				adapterId,
				null);
	}

	@Override
	public CloseableIterator<DataAdapter<?>> getAdapters() {
		return getObjects();
	}

	@Override
	protected ByteArrayId getPrimaryId(
			DataAdapter<?> persistedObject ) {
		return persistedObject.getAdapterId();
	}

	@Override
	protected String getPersistenceTypeName() {
		return ADAPTER_CF;
	}

}
