package mil.nga.giat.geowave.cli.debug;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.geotools.geometry.jts.Decimator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.renderer.lite.RendererUtilities;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.operation.MathTransform2D;
import org.opengis.referencing.operation.NoninvertibleTransformException;
import org.opengis.referencing.operation.TransformException;

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
	private static Logger LOGGER = Logger.getLogger(BBOXQuery.class);

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
		"-t",
		"--tileSize"
	}, required = false, description = "Tile size for subsampling")
	private Integer tileSize;

	@Parameter(names = {
		"-p",
		"--pixelSize"
	}, required = false, description = "Pixel size for subsampling")
	private Integer pixelSize;

	@Parameter(names = {
		"--useAggregation",
		"-agg"
	}, required = false, description = "Compute count on the server side")
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

			final QueryOptions queryOptions = new QueryOptions(
					adapterId,
					indexId);

			if (tileSize != null) {
				try {
					queryOptions.setMaxResolutionSubsamplingPerDimension(getSpans());
				}
				catch (Exception e) {
					LOGGER.error(e);
				}
			}

			CloseableIterator<Object> it = dataStore.query(
					queryOptions,
					new SpatialQuery(
							geom));

			stopWatch.stop();
			LOGGER.warn("Ran BBOX query in " + stopWatch.toString());

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
			LOGGER.warn("BBOX query results iteration took " + stopWatch.toString());
		}
		return count;
	}

	private double[] getSpans()
			throws MismatchedDimensionException,
			NoSuchAuthorityCodeException,
			FactoryException,
			NoninvertibleTransformException,
			TransformException {
		final AffineTransform worldToScreen = RendererUtilities.worldToScreenTransform(
				new ReferencedEnvelope(
						new Envelope(
								west,
								east,
								south,
								north),
						CRS.decode("EPSG:4326")),
				new Rectangle(
						tileSize,
						tileSize));
		final MathTransform2D fullTransform = (MathTransform2D) ProjectiveTransform.create(worldToScreen);

		final double[] spans = Decimator.computeGeneralizationDistances(
				fullTransform.inverse(),
				new Rectangle(
						tileSize,
						tileSize),
				pixelSize);

		return spans;
	}

}
