package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.util.SecondaryIndexEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloSecondaryIndexUtils;

/**
 * To be used when dealing with either a 'FULL' or 'PARTIAL' secondary index
 * type
 */
public class AccumuloSecondaryIndexEntryIteratorWrapper<T> extends
		SecondaryIndexEntryIteratorWrapper<T, T>
{

	private final static Logger LOGGER = Logger.getLogger(AccumuloSecondaryIndexEntryIteratorWrapper.class);
	private final Scanner scanner;

	public AccumuloSecondaryIndexEntryIteratorWrapper(
			final Scanner scanner,
			final DataAdapter<T> adapter ) {
		super(
				scanner.iterator(),
				adapter);
		this.scanner = scanner;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected T decodeRow(
			final Object row ) {
		Entry<Key, Value> entry = null;
		try {
			entry = (Entry<Key, Value>) row;
		}
		catch (final ClassCastException e) {
			LOGGER.error("Row is not an accumulo row entry.");
			return null;
		}
		return AccumuloSecondaryIndexUtils.decodeRow(
				entry.getKey(),
				entry.getValue(),
				adapter);
	}

	@Override
	public void close()
			throws IOException {
		scanner.close();
	}

}
