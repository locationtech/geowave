package mil.nga.giat.geowave.cli.debug;

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
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

public class ClientSideCQLQuery extends
		AbstractGeoWaveQuery
{
	private Filter filter;

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
		try {
			filter = ECQL.toFilter(commandLine.getOptionValue(
					"cql").toString());
		}
		catch (final CQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final DataStore dataStore,
			final boolean debug ) {
		long count = 0;
		try (final CloseableIterator<Object> it = dataStore.query(
				new QueryOptions(),
				null)) {
			while (it.hasNext()) {
				final Object o = it.next();
				if (o instanceof SimpleFeature) {
					if (filter.evaluate(o)) {
						if (debug) {
							System.out.println(o);
						}
						count++;
					}
				}
			}
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
		return count;
	}

}
