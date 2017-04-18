package mil.nga.giat.geowave.cli.debug;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.geotools.filter.text.cql2.CQLException;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

@GeowaveOperation(name = "cqlDelete", parentOperation = DebugSection.class)
@Parameters(commandDescription = "cql delete")
public class CQLDelete extends
		AbstractGeoWaveQuery
{
	private static Logger LOGGER = Logger.getLogger(CQLDelete.class);

	@Parameter(names = "--cql", required = true, description = "CQL Filter for delete")
	private String cqlStr;

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final DataStore dataStore,
			final boolean debug ) {
		long count = 0;

		try {
			long startMillis = System.currentTimeMillis();
			boolean success = dataStore.delete(
					new QueryOptions(
							adapterId,
							indexId),
					mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery.createOptimalQuery(
							cqlStr,
							adapter,
							null,
							null));

			LOGGER.warn("CQL Delete " + (success ? "Success" : "Failure") + " in "
					+ (System.currentTimeMillis() - startMillis) + "ms");
		}
		catch (CQLException e2) {
			LOGGER.warn(
					"Error parsing CQL",
					e2);
		}

		try (final CloseableIterator<Object> it = dataStore.query(
				new QueryOptions(
						adapterId,
						indexId),
				mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery.createOptimalQuery(
						cqlStr,
						adapter,
						null,
						null))) {

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
			LOGGER.warn(
					"Unable to read result",
					e);
		}
		catch (final CQLException e1) {
			LOGGER.error(
					"Unable to create optimal query",
					e1);
		}

		return count;
	}

}