package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.log4j.Logger;

import com.google.common.io.BaseEncoding;

public class SingleEntryFilterIterator extends
		Filter
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloDataStore.class);
	public static final String ENTRY_FILTER_ITERATOR_NAME = "GEOWAVE_ENTRY_FILTER_ITERATOR";
	public static final int ENTRY_FILTER_ITERATOR_PRIORITY = 15;
	public static final String WHOLE_ROW_ITERATOR_NAME = "GEOWAVE_WHOLE_ROW_ITERATOR";
	public static final int WHOLE_ROW_ITERATOR_PRIORITY = ENTRY_FILTER_ITERATOR_PRIORITY - 1;
	public static final String ADAPTER_ID = "adapterid";
	public static final String DATA_ID = "dataid";
	private byte[] adapterId;
	private byte[] dataId;

	@Override
	public boolean accept(
			final Key k,
			final Value v ) {

		boolean accept = true;

		Map<Key, Value> entries = null;
		try {
			entries = WholeRowIterator.decodeRow(
					k,
					v);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to decode row.",
					e);
			return false;
		}

		if ((entries != null) && entries.isEmpty()) {
			accept = false;
		}
		else {
			if (entries == null) {
				LOGGER.error("Internal error in iterator - entries map null when it shouldn't be");
				return false;
			}
			for (final Key key : entries.keySet()) {
				final byte[] localAdapterId = key.getColumnFamilyData().getBackingArray();

				if (Arrays.equals(
						localAdapterId,
						adapterId)) {
					final byte[] accumRowId = key.getRowData().getBackingArray();

					final byte[] metadata = Arrays.copyOfRange(
							accumRowId,
							accumRowId.length - 12,
							accumRowId.length);

					final ByteBuffer metadataBuf = ByteBuffer.wrap(metadata);
					final int adapterIdLength = metadataBuf.getInt();
					final int dataIdLength = metadataBuf.getInt();

					final ByteBuffer buf = ByteBuffer.wrap(
							accumRowId,
							0,
							accumRowId.length - 12);
					final byte[] indexId = new byte[accumRowId.length - 12 - adapterIdLength - dataIdLength];
					final byte[] rawAdapterId = new byte[adapterIdLength];
					final byte[] rawDataId = new byte[dataIdLength];
					buf.get(indexId);
					buf.get(rawAdapterId);
					buf.get(rawDataId);

					if (!Arrays.equals(
							rawDataId,
							dataId) && Arrays.equals(
							rawAdapterId,
							adapterId)) {
						accept = false;
					}
				}
				else {
					accept = false;
				}
			}
		}

		return accept;
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {

		final String adapterIdStr = options.get(ADAPTER_ID);
		final String dataIdStr = options.get(DATA_ID);
		if (adapterIdStr == null) {
			throw new IllegalArgumentException(
					"'adapterid' must be set for " + SingleEntryFilterIterator.class.getName());
		}
		if (dataIdStr == null) {
			throw new IllegalArgumentException(
					"'dataid' must be set for " + SingleEntryFilterIterator.class.getName());
		}

		adapterId = BaseEncoding.base64Url().decode(
				adapterIdStr);
		dataId = BaseEncoding.base64Url().decode(
				dataIdStr);

		super.init(
				source,
				options,
				env);
	}
}
