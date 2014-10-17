package mil.nga.giat.geowave.vector.query;

import java.util.Collection;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.query.AccumuloRowIdsQuery;
import mil.nga.giat.geowave.accumulo.util.TransformerWriter;
import mil.nga.giat.geowave.accumulo.util.VisibilityTransformer;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;

/**
 * Used to remove the transaction id from the visibility of data fields for
 * specific row IDS
 * 
 */
public class TransformingVisibilityQuery extends
		AccumuloRowIdsQuery
{

	private final VisibilityTransformer transformer;

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
