package mil.nga.giat.geowave.cli.scratch;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.geotools.filter.text.cql2.CQLException;

public class CQLQuery extends
		AbstractGeoWaveQuery
{
	private String cqlStr;

	@Override
	protected void applyOptions(
			final Options options ) {
		final Option cql = new Option(
				"cql",
				true,
				"CQL Filter executed client side");
		cql.setRequired(true);
		options.addOption(cql);
	}

	@Override
	protected void parseOptions(
			final CommandLine commandLine ) {
		cqlStr = commandLine.getOptionValue(
				"cql").toString();
	}

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final DataStore dataStore,
			final boolean debug ) {
		long count = 0;
		try (final CloseableIterator<Object> it = dataStore.query(
				new QueryOptions(
						adapterId,
						null),
				new mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery(
						cqlStr,
						adapter))) {
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
		catch (final CQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return count;
	}
}