package mil.nga.giat.geowave.cli.debug;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.geotools.filter.text.cql2.CQLException;

public class CQLQuery extends
		AbstractGeoWaveQuery
{
	private String cqlStr;
	private boolean useAggregation = false;

	@Override
	protected void applyOptions(
			final Options options ) {
		final Option cql = new Option(
				"cql",
				true,
				"CQL Filter executed client side");
		cql.setRequired(true);
		options.addOption(cql);

		final Option aggregation = new Option(
				"useAggregation",
				false,
				"Compute count on the server side");
		aggregation.setRequired(false);
		options.addOption(aggregation);
	}

	@Override
	protected void parseOptions(
			final CommandLine commandLine ) {
		cqlStr = commandLine.getOptionValue(
				"cql").toString();
		useAggregation = commandLine.hasOption("useAggregation");
	}

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final DataStore dataStore,
			final boolean debug ) {
		long count = 0;
		if (useAggregation) {
			final QueryOptions options = new QueryOptions(
					adapterId,
					indexId);
			options.setAggregation(
					new CountAggregation(),
					adapter);
			try (final CloseableIterator<Object> it = dataStore.query(
					options,
					new mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery(
							cqlStr,
							adapter))) {
				count += ((CountAggregation) (it.next())).getCount();
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
		else {
			try (final CloseableIterator<Object> it = dataStore.query(
					new QueryOptions(
							adapterId,
							indexId),
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
}