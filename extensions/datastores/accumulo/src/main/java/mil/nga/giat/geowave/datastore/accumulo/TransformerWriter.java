package mil.nga.giat.geowave.datastore.accumulo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

public class TransformerWriter
{
	private final static Logger LOGGER = Logger.getLogger(TransformerWriter.class);
	private final ScannerBase scanner;
	private final String tableName;
	private final AccumuloOperations accumuloOperations;

	public TransformerWriter(
			ScannerBase scanner,
			String tableName,
			AccumuloOperations accumuloOperations ) {
		super();
		this.scanner = scanner;
		this.tableName = tableName;
		this.accumuloOperations = accumuloOperations;
	}

	public void transform() {
		try {
			final Iterator<Entry<Key, Value>> rawIt = scanner.iterator();
			Writer writer = accumuloOperations.createWriter(tableName);
			writer.write(new Iterable<Mutation>() {
				@Override
				public Iterator<Mutation> iterator() {
					return new Iterator<Mutation>() {
						List<Entry<Key, Value>> lastEntries = new ArrayList<Entry<Key, Value>>();
						LinkedList<Mutation> pendingOutput = new LinkedList<Mutation>();
						byte[] rowID = null;

						@Override
						public boolean hasNext() {
							return !pendingOutput.isEmpty() || rawIt.hasNext() || !lastEntries.isEmpty();
						}

						@Override
						public Mutation next() {
							boolean ignoreLastEntry = false;
							if (!pendingOutput.isEmpty()) {
								return pendingOutput.removeFirst();
							}
							else if (rawIt.hasNext()) {
								while (rawIt.hasNext()) {
									Entry<Key, Value> entry = rawIt.next();
									addEntry(
											lastEntries,
											entry);
									byte[] currentRowID = entry.getKey().getRow().getBytes();
									if (rowID == null || Arrays.equals(
											rowID,
											currentRowID)) {
										rowID = currentRowID;
									}
									else {
										rowID = currentRowID;
										ignoreLastEntry = true;
										break;
									}
								}
							}

							// either out of entries in the rawIt OR rowID
							// changed.
							int countDown = lastEntries.size();
							Iterator<Entry<Key, Value>> it = lastEntries.iterator();
							while (it.hasNext()) {
								Entry<Key, Value> lastEntry = it.next();
								countDown--;
								// the last entry in lastEntries has a different
								// row ID.
								if (ignoreLastEntry && countDown == 0) break;

								Mutation mutation = new Mutation(
										lastEntry.getKey().getRow());
								if (lastEntry.getKey().isDeleted()) {
									mutation.putDelete(
											lastEntry.getKey().getColumnFamily(),
											lastEntry.getKey().getColumnQualifier(),
											lastEntry.getKey().getColumnVisibilityParsed(),
											lastEntry.getKey().getTimestamp());
								}
								else {
									mutation.put(
											lastEntry.getKey().getColumnFamily(),
											lastEntry.getKey().getColumnQualifier(),
											lastEntry.getKey().getColumnVisibilityParsed(),
											lastEntry.getValue());
								}
								pendingOutput.addLast(mutation);
								it.remove();
							}

							return pendingOutput.removeFirst();
						}

						@Override
						public void remove() {

						}
					};
				}
			});
			writer.close();
		}
		catch (Exception ex) {
			LOGGER.error(
					"Cannot perform transformation",
					ex);
		}
	}

	private static void addEntry(
			List<Entry<Key, Value>> others,
			Entry<Key, Value> entry ) {
		for (Entry<Key, Value> other : others) {
			if (other.getKey().getRow().equals(
					entry.getKey().getRow()) && other.getKey().getColumnQualifier().equals(
					entry.getKey().getColumnQualifier()) && other.getKey().getColumnFamily().equals(
					entry.getKey().getColumnFamily())) {
				if (other.getKey().getTimestamp() < entry.getKey().getTimestamp())
					other.getKey().setDeleted(
							true);
				else
					entry.getKey().setDeleted(
							true);
			}
		}
		others.add(entry);
	}
}
