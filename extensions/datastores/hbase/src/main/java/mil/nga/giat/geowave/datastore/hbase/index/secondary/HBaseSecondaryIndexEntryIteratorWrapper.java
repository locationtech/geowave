package mil.nga.giat.geowave.datastore.hbase.index.secondary;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.SecondaryIndexEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseSecondaryIndexUtils;

/**
 * To be used when dealing with either a 'FULL' or 'PARTIAL' secondary index
 * type
 */
public class HBaseSecondaryIndexEntryIteratorWrapper<T> extends
		SecondaryIndexEntryIteratorWrapper<T, Object>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseSecondaryIndexEntryIteratorWrapper.class);
	private final ResultScanner scanner;
	private final byte[] columnFamily;
	private final PrimaryIndex index;

	public HBaseSecondaryIndexEntryIteratorWrapper(
			final ResultScanner scanner,
			final byte[] columnFamily,
			final DataAdapter<T> adapter,
			final PrimaryIndex index ) {
		super(
				scanner.iterator(),
				adapter);
		this.scanner = scanner;
		this.columnFamily = columnFamily;
		this.index = index;
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
				adapter,
				index);
	}

	@Override
	public void close()
			throws IOException {
		scanner.close();
	}

}
