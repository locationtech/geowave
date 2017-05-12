package mil.nga.giat.geowave.datastore.accumulo.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;

/**
 * Unlike the transform iterator, this transform works on the client side
 * deleting to original cell and adding a new cell. The transformation is
 * carried out by a {@link Transformer}.
 * 
 */
public class TransformerWriter
{
	private final static Logger LOGGER = LoggerFactory.getLogger(TransformerWriter.class);
	private final ScannerBase scanner;
	private final String tableName;
	private final AccumuloOperations accumuloOperations;
	private final Transformer transformer;

	public TransformerWriter(
			final ScannerBase scanner,
			final String tableName,
			final AccumuloOperations accumuloOperations,
			final Transformer transformer ) {
		super();
		this.scanner = scanner;
		this.tableName = tableName;
		this.accumuloOperations = accumuloOperations;
		this.transformer = transformer;
	}

	public void transform() {
		try {
			final Iterator<Entry<Key, Value>> rawIt = scanner.iterator();
			Writer writer = accumuloOperations.createWriter(tableName);
			writer.write(new Iterable<Mutation>() {
				@Override
				public Iterator<Mutation> iterator() {
					return new Iterator<Mutation>() {

						LinkedList<Mutation> pendingOutput = new LinkedList<Mutation>();

						@Override
						public boolean hasNext() {
							return !pendingOutput.isEmpty() || rawIt.hasNext();
						}

						@Override
						public Mutation next() {

							if (!pendingOutput.isEmpty()) {
								return pendingOutput.removeFirst();
							}
							else if (rawIt.hasNext()) {
								Entry<Key, Value> entry = rawIt.next();
								Entry<Key, Value> newEntry = transformer.transform(Pair.of(
										entry.getKey(),
										entry.getValue()));

								Mutation mutation = new Mutation(
										entry.getKey().getRow());
								mutation.putDelete(
										entry.getKey().getColumnFamily(),
										entry.getKey().getColumnQualifier(),
										entry.getKey().getColumnVisibilityParsed(),
										entry.getKey().getTimestamp());

								pendingOutput.add(mutation);

								mutation = new Mutation(
										newEntry.getKey().getRow());

								mutation.put(
										newEntry.getKey().getColumnFamily(),
										newEntry.getKey().getColumnQualifier(),
										newEntry.getKey().getColumnVisibilityParsed(),
										newEntry.getValue());
								pendingOutput.add(mutation);
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
}
