package mil.nga.giat.geowave.vector.query.row;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.VisibilityTransformationIterator;
import mil.nga.giat.geowave.accumulo.Writer;
import mil.nga.giat.geowave.accumulo.query.AccumuloRowIdsQuery;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

/**
 * Used to remove the transaction id from the visibility of data fields.
 * 
 */
public class TransformingVisibilityQuery extends
		AccumuloRowIdsQuery
{

	private final static Logger LOGGER = Logger.getLogger(TransformingVisibilityQuery.class);

	private final String transformingRegex;
	private final String replacement;
	private final Class<? extends SortedKeyValueIterator<Key, Value>> transformIteratorClass;

	public TransformingVisibilityQuery(
			Class<? extends SortedKeyValueIterator<Key, Value>> transformIteratorClass,
			Index index,
			Collection<ByteArrayId> rows,
			String[] authorizations,
			String transformingRegex,
			String replacement ) {
		super(
				index,
				rows,
				authorizations);
		this.transformingRegex = transformingRegex;
		this.replacement = replacement;
		this.transformIteratorClass = transformIteratorClass;
	}

	@Override
	protected void addScanIteratorSettings(
			ScannerBase scanner ) {
		super.addScanIteratorSettings(scanner);
		final IteratorSetting iteratorSettings = new IteratorSetting(
				10,
				"GEOWAVE_TRANSFORM",
				(Class<? extends SortedKeyValueIterator<Key, Value>>) transformIteratorClass);
		VisibilityTransformationIterator.populate(
				iteratorSettings,
				this.getAdditionalAuthorizations(),
				transformingRegex,
				replacement);
		scanner.addScanIterator(iteratorSettings);
	}

	public void run(
			final AccumuloOperations accumuloOperations,
			final AdapterStore adapterStore,
			final Integer limit )
			throws TableNotFoundException {

	}

	public CloseableIterator<Boolean> query(
			final AccumuloOperations accumuloOperations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		final ScannerBase scanner = getScanner(
				accumuloOperations,
				limit);
		addScanIteratorSettings(scanner);
		final Iterator<Entry<Key, Value>> rawIt = scanner.iterator();
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		try {
			Writer writer = accumuloOperations.createWriter(tableName);
			writer.write(new Iterable<Mutation>() {
				@Override
				public Iterator<Mutation> iterator() {
					return new Iterator<Mutation>() {

						@Override
						public boolean hasNext() {
							return rawIt.hasNext();
						}

						@Override
						public Mutation next() {
							Entry<Key, Value> entry = rawIt.next();
							Mutation mutation = new Mutation(
									entry.getKey().getRow());
							mutation.put(
									entry.getKey().getColumnFamily(),
									entry.getKey().getColumnQualifier(),
									entry.getKey().getColumnVisibilityParsed(),
									entry.getValue());
							return mutation;
						}

						@Override
						public void remove() {

						}
					};
				}
			});
			while (rawIt.hasNext()) {

			}
		}
		catch (Exception ex) {
			TransformingVisibilityQuery.LOGGER.error(
					"Cannot commit transaction",
					ex);
		}
		scanner.close();

		return new CloseableIterator<Boolean>() {

			Boolean result = new Boolean(
					true);

			@Override
			public boolean hasNext() {
				return result != null;
			}

			@Override
			public Boolean next() {
				return result;
			}

			@Override
			public void remove() {

			}

			@Override
			public void close()
					throws IOException {}

		};
	}
}
