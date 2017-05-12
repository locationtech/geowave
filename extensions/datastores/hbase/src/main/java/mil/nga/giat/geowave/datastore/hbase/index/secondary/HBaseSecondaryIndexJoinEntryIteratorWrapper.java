package mil.nga.giat.geowave.datastore.hbase.index.secondary;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexUtils;
import mil.nga.giat.geowave.core.store.util.SecondaryIndexEntryIteratorWrapper;

/**
 * To be used when dealing with a 'JOIN' secondary index type
 */
public class HBaseSecondaryIndexJoinEntryIteratorWrapper<T> extends
		SecondaryIndexEntryIteratorWrapper<T, Pair<ByteArrayId, ByteArrayId>>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseSecondaryIndexJoinEntryIteratorWrapper.class);
	private final ResultScanner scanner;
	private final byte[] columnFamily;

	public HBaseSecondaryIndexJoinEntryIteratorWrapper(
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
	protected Pair<ByteArrayId, ByteArrayId> decodeRow(
			final Object row ) {
		Result entry = null;
		try {
			entry = (Result) row;
		}
		catch (final ClassCastException e) {
			LOGGER.error("Row is not a hbase row entry.");
			return null;
		}
		final NavigableMap<byte[], byte[]> qualifierToValueMap = entry.getFamilyMap(columnFamily);
		final Set<Entry<byte[], byte[]>> entries = qualifierToValueMap.entrySet();
		for (final Entry<byte[], byte[]> curr : entries) {
			final byte[] cq = curr.getKey();
			// there will only be one entry in the map for a join index
			return Pair.of(
					SecondaryIndexUtils.getPrimaryIndexId(cq),
					SecondaryIndexUtils.getPrimaryRowId(cq));
		}
		return null;
	}

	@Override
	public void close()
			throws IOException {
		scanner.close();
	}

}
