package mil.nga.giat.geowave.datastore.hbase.index.secondary;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.util.SecondaryIndexEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseSecondaryIndexUtils;

/**
 * To be used when dealing with either a 'FULL' or 'PARTIAL' secondary index
 * type
 */
public class HBaseSecondaryIndexEntryIteratorWrapper<T> extends
		SecondaryIndexEntryIteratorWrapper<T, Object>
{
	private final static Logger LOGGER = Logger.getLogger(HBaseSecondaryIndexEntryIteratorWrapper.class);
	private final ResultScanner scanner;
	private final byte[] columnFamily;

	public HBaseSecondaryIndexEntryIteratorWrapper(
			final ResultScanner scanner,
			final byte[] columnFamily,
			final DataAdapter<T> adapter ) {
		super(
				scanner.iterator(),
				adapter);
		this.scanner = scanner;
		this.columnFamily = columnFamily;
	}

	@Override
	protected T decodeRow(
			final Object row ) {
		Result entry = null;
		try {
			entry = (Result) row;
		}
		catch (final ClassCastException e) {
			LOGGER.error("Row is not a hbase row entry.");
			return null;
		}
		return HBaseSecondaryIndexUtils.decodeRow(
				entry,
				columnFamily,
				adapter);
	}

	@Override
	public void close()
			throws IOException {
		scanner.close();
	}

}
