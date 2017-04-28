package mil.nga.giat.geowave.cli.debug;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Stopwatch;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

@GeowaveOperation(name = "bbox", parentOperation = DebugSection.class)
@Parameters(commandDescription = "bbox query")
public class BBOXQuery extends
		AbstractGeoWaveQuery
{
	private static Logger LOGGER = LoggerFactory.getLogger(BBOXQuery.class);

	@Parameter(names = {
		"-e",
		"--east"
	}, required = true, description = "Max Longitude of BBOX")
	private Double east;

	@Parameter(names = {
		"-w",
		"--west"
	}, required = true, description = "Min Longitude of BBOX")
	private Double west;

	@Parameter(names = {
		"-n",
		"--north"
	}, required = true, description = "Max Latitude of BBOX")
	private Double north;

	@Parameter(names = {
		"-s",
		"--south"
	}, required = true, description = "Min Latitude of BBOX")
	private Double south;

	@Parameter(names = {
		"--useAggregation",
		"-agg"
	}, description = "Compute count on the server side")
	private Boolean useAggregation = Boolean.FALSE;

	private Geometry geom;

	private void getBoxGeom() {
		geom = new GeometryFactory().toGeometry(new Envelope(
				west,
				east,
				south,
				north));
	}

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final DataStore dataStore,
			final boolean debug ) {
		final Stopwatch stopWatch = new Stopwatch();

		getBoxGeom();

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
					new SpatialQuery(
							geom))) {
				final CountResult result = ((CountResult) (it.next()));
				if (result != null) {
					count += result.getCount();
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to read result",
						e);
			}
		}
		else {
			stopWatch.start();

			CloseableIterator<Object> it = dataStore.query(
					new QueryOptions(
							adapterId,
							indexId),
					new SpatialQuery(
							geom));

			stopWatch.stop();
			System.out.println("Ran BBOX query in " + stopWatch.toString());

			stopWatch.reset();
			stopWatch.start();

			while (it.hasNext()) {
				if (debug) {
					System.out.println(it.next());
				}
				else {
					it.next();
				}
				count++;
			}

			stopWatch.stop();
			System.out.println("BBOX query results iteration took " + stopWatch.toString());
		}
		return count;
	}

}
