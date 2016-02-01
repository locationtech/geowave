package mil.nga.giat.geowave.cli.debug;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class FullTableScan extends
		AbstractGeoWaveQuery
{

	@Override
	protected void applyOptions(
			final Options options ) {}

	@Override
	protected void parseOptions(
			final CommandLine commandLine ) {}

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final DataStore dataStore,
			final boolean debug ) {
		long count = 0;
		try (final CloseableIterator<Object> it = dataStore.query(
				new QueryOptions(
						adapterId,
						indexId),
				null)) {
			while (it.hasNext()) {
				if (debug) {
					System.out.println(it.next());
				}
				else {
					it.next();
				}
				count++;
			}

		}
		catch (final IOException e) {
			e.printStackTrace();
		}
		return count;
	}

}
