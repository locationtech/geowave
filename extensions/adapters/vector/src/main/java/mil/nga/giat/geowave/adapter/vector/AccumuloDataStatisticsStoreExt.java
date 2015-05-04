package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.util.TransformerWriter;
import mil.nga.giat.geowave.datastore.accumulo.util.VisibilityTransformer;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * This extension supports transforming the visibility of statistics associated
 * with transactions.
 * 
 * 
 */
public class AccumuloDataStatisticsStoreExt extends
		AccumuloDataStatisticsStore
{

	private final static Logger LOGGER = Logger.getLogger(AccumuloDataStatisticsStoreExt.class);
	private AccumuloOperations accumuloOperations;

	public AccumuloDataStatisticsStoreExt(
			AccumuloOperations accumuloOperations ) {
		super(
				accumuloOperations);
		this.accumuloOperations = accumuloOperations;
	}

	public void transformVisibility(
			final ByteArrayId adapterId,
			final VisibilityTransformer visibilityTransformer,
			String... authorizations ) {
		Scanner scanner;

		try {
			scanner = createSortScanner(
					adapterId,
					authorizations);

			TransformerWriter writer = new TransformerWriter(
					scanner,
					getAccumuloTablename(),
					accumuloOperations,
					visibilityTransformer);
			writer.transform();
			scanner.close();
		}
		catch (TableNotFoundException e) {
			LOGGER.error(
					"Table not found during transaction commit: " + getAccumuloTablename(),
					e);
		}
	}

	private Scanner createSortScanner(
			final ByteArrayId adapterId,
			String... authorizations )
			throws TableNotFoundException {
		Scanner scanner = null;

		scanner = accumuloOperations.createScanner(
				getAccumuloTablename(),
				authorizations);

		final IteratorSetting[] settings = getScanSettings();
		if ((settings != null) && (settings.length > 0)) {
			for (final IteratorSetting setting : settings) {
				scanner.addScanIterator(setting);
			}
		}
		final String columnFamily = getAccumuloColumnFamily();
		final String columnQualifier = getAccumuloColumnQualifier(adapterId);
		scanner.fetchColumn(
				new Text(
						columnFamily),
				new Text(
						columnQualifier));

		// scanner.setRange(Range.);
		return scanner;
	}

}
