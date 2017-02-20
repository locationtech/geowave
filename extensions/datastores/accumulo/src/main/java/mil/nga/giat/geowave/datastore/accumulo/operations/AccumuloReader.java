package mil.nga.giat.geowave.datastore.accumulo.operations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRow;

public class AccumuloReader implements
		Reader
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloReader.class);
	private final ScannerBase scanner;
	private final Iterator<Entry<Key, Value>> it;

	private final boolean wholeRowEncoding;
	private final int partitionKeyLength;

	public AccumuloReader(
			final ScannerBase scanner,
			final int partitionKeyLength,
			final boolean wholeRowEncoding ) {
		this.scanner = scanner;
		this.partitionKeyLength = partitionKeyLength;
		this.wholeRowEncoding = wholeRowEncoding;

		it = scanner.iterator();
	}

	@Override
	public void close()
			throws Exception {
		scanner.close();
	}

	@Override
	public boolean hasNext() {
		return it.hasNext();
	}

	@Override
	public GeoWaveRow next() {
		final Entry<Key, Value> entry = it.next();
		Map<Key, Value> rowMapping;
		if (wholeRowEncoding) {
			try {
				rowMapping = WholeRowIterator.decodeRow(
						entry.getKey(),
						entry.getValue());
			}
			catch (final IOException e) {
				LOGGER.error(
						"Could not decode row from iterator. Ensure whole row iterators are being used.",
						e);
				return null;
			}
		}
		else {
			rowMapping = new HashMap<Key, Value>();
			rowMapping.put(
					entry.getKey(),
					entry.getValue());
		}
		return new AccumuloRow(
				entry.getKey().getRow().copyBytes(),
				partitionKeyLength,
				rowMapping);
	}

}
