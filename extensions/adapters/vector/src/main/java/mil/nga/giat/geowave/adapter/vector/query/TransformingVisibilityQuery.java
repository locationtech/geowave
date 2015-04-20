package mil.nga.giat.geowave.adapter.vector.query;

import java.util.Collection;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRowIdsQuery;
import mil.nga.giat.geowave.datastore.accumulo.util.TransformerWriter;
import mil.nga.giat.geowave.datastore.accumulo.util.VisibilityTransformer;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;

/**
 * Used to remove the transaction id from the visibility of data fields for
 * specific row IDS
 * 
 */
public class TransformingVisibilityQuery extends
		AccumuloRowIdsQuery
{

	private final VisibilityTransformer transformer;
	private static final Logger LOGGER = Logger.getLogger(TransformingVisibilityQuery.class);

	public TransformingVisibilityQuery(
			final VisibilityTransformer transformer,
			final Index index,
			final Collection<ByteArrayId> rows,
			final String[] authorizations ) {
		super(
				index,
				rows,
				authorizations);
		this.transformer = transformer;
	}

	@Override
	protected void addScanIteratorSettings(
			ScannerBase scanner ) {
		super.addScanIteratorSettings(scanner);
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
		if (scanner == null) {
			LOGGER.error("Could not get scanner instance, getScanner returned null");
			return new CloseableIterator.Empty<Boolean>();
		}
		addScanIteratorSettings(scanner);
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		TransformerWriter writer = new TransformerWriter(
				scanner,
				tableName,
				accumuloOperations,
				transformer);
		writer.transform();
		scanner.close();

		return new CloseableIterator.Empty<Boolean>();
	}
}
