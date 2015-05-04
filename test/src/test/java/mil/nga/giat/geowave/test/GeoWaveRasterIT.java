package mil.nga.giat.geowave.test;

import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;
import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.adapter.raster.adapter.MergeableRasterTile;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.SimpleAbstractMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.query.IndexOnlySpatialQuery;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.util.ConnectorPool;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.Test;
import org.opengis.coverage.grid.GridCoverage;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

public class GeoWaveRasterIT extends
		GeoWaveTestEnvironment
{
	private static final double DOUBLE_TOLERANCE = 1E-10d;

	@Test
	public void testNoDataMergeStrategy()
			throws IOException,
			AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException {
		final String coverageName = "testNoDataMergeStrategy";
		final int tileSize = 256;
		final double westLon = 0;
		final double eastLon = 45;
		final double southLat = 0;
		final double northLat = 45;
		ingestAndQueryNoDataMergeStrategy(
				coverageName,
				tileSize,
				westLon,
				eastLon,
				southLat,
				northLat);
	}

	@Test
	public void testMultipleMergeStrategies()
			throws IOException,
			AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException {
		final String noDataCoverageName = "testMultipleMergeStrategies_NoDataMergeStrategy";
		final String summingCoverageName = "testMultipleMergeStrategies_SummingMergeStrategy";
		final String sumAndAveragingCoverageName = "testMultipleMergeStrategies_SumAndAveragingMergeStrategy";
		final int summingNumBands = 8;
		final int summingNumRasters = 4;

		final int sumAndAveragingNumBands = 12;
		final int sumAndAveragingNumRasters = 15;
		final int noDataTileSize = 64;
		final int summingTileSize = 32;
		final int sumAndAveragingTileSize = 8;
		final double westLon = 45;
		final double eastLon = 47.8125;
		final double southLat = -47.8125;
		final double northLat = -45;
		ingestGeneralPurpose(
				summingCoverageName,
				summingTileSize,
				westLon,
				eastLon,
				southLat,
				northLat,
				summingNumBands,
				summingNumRasters,
				new SummingMergeStrategy());
		ingestNoDataMergeStrategy(
				noDataCoverageName,
				noDataTileSize,
				westLon,
				eastLon,
				southLat,
				northLat);
		ingestGeneralPurpose(
				sumAndAveragingCoverageName,
				sumAndAveragingTileSize,
				westLon,
				eastLon,
				southLat,
				northLat,
				sumAndAveragingNumBands,
				sumAndAveragingNumRasters,
				new SumAndAveragingMergeStrategy());

		queryNoDataMergeStrategy(
				noDataCoverageName,
				noDataTileSize);
		queryGeneralPurpose(
				summingCoverageName,
				summingTileSize,
				westLon,
				eastLon,
				southLat,
				northLat,
				summingNumBands,
				summingNumRasters,
				new SummingExpectedValue());
		queryGeneralPurpose(
				sumAndAveragingCoverageName,
				sumAndAveragingTileSize,
				westLon,
				eastLon,
				southLat,
				northLat,
				sumAndAveragingNumBands,
				sumAndAveragingNumRasters,
				new SumAndAveragingExpectedValue());

		// get a connector so we can make sure queries work before and after
		// compaction
		final Connector connector = ConnectorPool.getInstance().getConnector(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword);
		connector.tableOperations().compact(
				TEST_NAMESPACE + "_" + IndexType.SPATIAL_RASTER.createDefaultIndex().getId().getString(),
				null,
				null,
				true,
				true);
		// test query again after compaction
		queryNoDataMergeStrategy(
				noDataCoverageName,
				noDataTileSize);
		queryGeneralPurpose(
				summingCoverageName,
				summingTileSize,
				westLon,
				eastLon,
				southLat,
				northLat,
				summingNumBands,
				summingNumRasters,
				new SummingExpectedValue());
		queryGeneralPurpose(
				sumAndAveragingCoverageName,
				sumAndAveragingTileSize,
				westLon,
				eastLon,
				southLat,
				northLat,
				summingNumBands,
				sumAndAveragingNumRasters,
				new SumAndAveragingExpectedValue());

	}

	private void ingestAndQueryNoDataMergeStrategy(
			final String coverageName,
			final int tileSize,
			final double westLon,
			final double eastLon,
			final double southLat,
			final double northLat )
			throws IOException,
			AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException {
		ingestNoDataMergeStrategy(
				coverageName,
				tileSize,
				westLon,
				eastLon,
				southLat,
				northLat);
		queryNoDataMergeStrategy(
				coverageName,
				tileSize);
		// get a connector so we can make sure queries work before and after
		// compaction
		final Connector connector = ConnectorPool.getInstance().getConnector(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword);
		connector.tableOperations().compact(
				TEST_NAMESPACE + "_" + IndexType.SPATIAL_RASTER.createDefaultIndex().getId().getString(),
				null,
				null,
				true,
				true);
		// test query again after compaction
		queryNoDataMergeStrategy(
				coverageName,
				tileSize);
	}

	private void queryNoDataMergeStrategy(
			final String coverageName,
			final int tileSize )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		final DataStore dataStore = new AccumuloDataStore(
				accumuloOperations);
		final List<ByteArrayId> ids = new ArrayList<ByteArrayId>();
		ids.add(new ByteArrayId(
				coverageName));
		try (CloseableIterator<?> it = dataStore.query(
				ids,
				null)) {
			// the expected outcome is:
			// band 1,2,3,4,5,6 has every value set correctly, band 0 has every
			// even row set correctly and every odd row should be NaN, and band
			// 7 has the upper quadrant as NaN and the rest set
			final GridCoverage coverage = (GridCoverage) it.next();
			final Raster raster = coverage.getRenderedImage().getData();

			Assert.assertEquals(
					tileSize,
					raster.getWidth());
			Assert.assertEquals(
					tileSize,
					raster.getHeight());
			for (int x = 0; x < tileSize; x++) {
				for (int y = 0; y < tileSize; y++) {

					for (int b = 1; b < 7; b++) {
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=" + b,
								getValue(
										x,
										y,
										b,
										tileSize),
								raster.getSampleDouble(
										x,
										y,
										b));

					}
					if ((y % 2) == 0) {
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=0",
								getValue(
										x,
										y,
										0,
										tileSize),
								raster.getSampleDouble(
										x,
										y,
										0));
					}
					else {
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=0",
								Double.NaN,
								raster.getSampleDouble(
										x,
										y,
										0));
					}
					if ((x > ((tileSize * 3) / 4)) && (y > ((tileSize * 3) / 4))) {
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=7",
								Double.NaN,
								raster.getSampleDouble(
										x,
										y,
										7));
					}
					else {
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=7",
								getValue(
										x,
										y,
										7,
										tileSize),
								raster.getSampleDouble(
										x,
										y,
										7));

					}
				}
			}

			// there should be exactly one
			Assert.assertFalse(it.hasNext());
		}
	}

	private void ingestNoDataMergeStrategy(
			final String coverageName,
			final int tileSize,
			final double westLon,
			final double eastLon,
			final double southLat,
			final double northLat ) {
		final int numBands = 8;
		final DataStore dataStore = new AccumuloDataStore(
				accumuloOperations);
		final RasterDataAdapter adapter = RasterUtils.createDataAdapterTypeDouble(
				coverageName,
				numBands,
				tileSize);
		final WritableRaster raster1 = RasterUtils.createRasterTypeDouble(
				numBands,
				tileSize);
		final WritableRaster raster2 = RasterUtils.createRasterTypeDouble(
				numBands,
				tileSize);
		// for raster1 do the following:
		// set every even row in bands 0 and 1
		// set every value incorrectly in band 2
		// set no values in band 3 and set every value in 4

		// for raster2 do the following:
		// set no value in band 0 and 4
		// set every odd row in band 1
		// set every value in bands 2 and 3

		// for band 5, set the lower 2x2 samples for raster 1 and the rest for
		// raster 2
		// for band 6, set the upper quadrant samples for raster 1 and the rest
		// for raster 2
		// for band 7, set the lower 2x2 samples to the wrong value for raster 1
		// and the expected value for raster 2 and set everything but the upper
		// quadrant for raster 2
		for (int x = 0; x < tileSize; x++) {
			for (int y = 0; y < tileSize; y++) {

				// just use x and y to arbitrarily end up with some wrong value
				// that can be ingested
				final double wrongValue = (getValue(
						y,
						x,
						y,
						tileSize) * 3) + 1;
				if ((x < 2) && (y < 2)) {
					raster1.setSample(
							x,
							y,
							5,
							getValue(
									x,
									y,
									5,
									tileSize));
					raster1.setSample(
							x,
							y,
							7,
							wrongValue);
					raster2.setSample(
							x,
							y,
							7,
							getValue(
									x,
									y,
									7,
									tileSize));
				}
				else {
					raster2.setSample(
							x,
							y,
							5,
							getValue(
									x,
									y,
									5,
									tileSize));
				}
				if ((x > ((tileSize * 3) / 4)) && (y > ((tileSize * 3) / 4))) {
					raster1.setSample(
							x,
							y,
							6,
							getValue(
									x,
									y,
									6,
									tileSize));
				}
				else {
					raster2.setSample(
							x,
							y,
							6,
							getValue(
									x,
									y,
									6,
									tileSize));
					raster2.setSample(
							x,
							y,
							7,
							getValue(
									x,
									y,
									7,
									tileSize));
				}
				if ((y % 2) == 0) {
					raster1.setSample(
							x,
							y,
							0,
							getValue(
									x,
									y,
									0,
									tileSize));
					raster1.setSample(
							x,
							y,
							1,
							getValue(
									x,
									y,
									1,
									tileSize));
				}
				raster1.setSample(
						x,
						y,
						2,
						wrongValue);

				raster1.setSample(
						x,
						y,
						4,
						getValue(
								x,
								y,
								4,
								tileSize));
				if ((y % 2) == 1) {
					raster2.setSample(
							x,
							y,
							1,
							getValue(
									x,
									y,
									1,
									tileSize));
				}
				raster2.setSample(
						x,
						y,
						2,
						getValue(
								x,
								y,
								2,
								tileSize));

				raster2.setSample(
						x,
						y,
						3,
						getValue(
								x,
								y,
								3,
								tileSize));
			}
		}

		dataStore.ingest(
				adapter,
				IndexType.SPATIAL_RASTER.createDefaultIndex(),
				RasterUtils.createCoverageTypeDouble(
						coverageName,
						westLon,
						eastLon,
						southLat,
						northLat,
						raster1));
		dataStore.ingest(
				adapter,
				IndexType.SPATIAL_RASTER.createDefaultIndex(),
				RasterUtils.createCoverageTypeDouble(
						coverageName,
						westLon,
						eastLon,
						southLat,
						northLat,
						raster2));
	}

	private void ingestGeneralPurpose(
			final String coverageName,
			final int tileSize,
			final double westLon,
			final double eastLon,
			final double southLat,
			final double northLat,
			final int numBands,
			final int numRasters,
			final RasterTileMergeStrategy<?> mergeStrategy ) {

		// just ingest a number of rasters
		final DataStore dataStore = new AccumuloDataStore(
				accumuloOperations);
		final RasterDataAdapter basicAdapter = RasterUtils.createDataAdapterTypeDouble(
				coverageName,
				numBands,
				tileSize);
		final RasterDataAdapter mergeStrategyOverriddenAdapter = new RasterDataAdapter(
				basicAdapter,
				coverageName,
				mergeStrategy);
		final Index index = IndexType.SPATIAL_RASTER.createDefaultIndex();
		for (int r = 0; r < numRasters; r++) {
			final WritableRaster raster = RasterUtils.createRasterTypeDouble(
					numBands,
					tileSize);
			for (int x = 0; x < tileSize; x++) {
				for (int y = 0; y < tileSize; y++) {
					for (int b = 0; b < numBands; b++) {
						raster.setSample(
								x,
								y,
								b,
								getValue(
										x,
										y,
										b,
										r,
										tileSize));
					}
				}
			}
			dataStore.ingest(
					mergeStrategyOverriddenAdapter,
					index,
					RasterUtils.createCoverageTypeDouble(
							coverageName,
							westLon,
							eastLon,
							southLat,
							northLat,
							raster));
		}
	}

	private static double getValue(
			final int x,
			final int y,
			final int b,
			final int tileSize ) {
		// just use an arbitrary 'r'
		return getValue(
				x,
				y,
				b,
				3,
				tileSize);
	}

	private static double getValue(
			final int x,
			final int y,
			final int b,
			final int r,
			final int tileSize ) {
		// make this some random but repeatable and vary the scale
		final double resultOfFunction = randomFunction(
				x,
				y,
				b,
				r,
				tileSize);
		// this is meant to just vary the scale
		if ((r % 2) == 0) {
			return resultOfFunction;
		}
		else {

			return new Random(
					(long) resultOfFunction).nextDouble() * resultOfFunction;
		}
	}

	private static double randomFunction(
			final int x,
			final int y,
			final int b,
			final int r,
			final int tileSize ) {
		return (((x + (y * tileSize)) * .1) / (b + 1)) + r;
	}

	private void queryGeneralPurpose(
			final String coverageName,
			final int tileSize,
			final double westLon,
			final double eastLon,
			final double southLat,
			final double northLat,
			final int numBands,
			final int numRasters,
			final ExpectedValue expectedValue )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		final DataStore dataStore = new AccumuloDataStore(
				accumuloOperations);
		final List<ByteArrayId> ids = new ArrayList<ByteArrayId>();
		ids.add(new ByteArrayId(
				coverageName));

		try (CloseableIterator<?> it = dataStore.query(
				ids,
				new IndexOnlySpatialQuery(
						new GeometryFactory().toGeometry(new Envelope(
								westLon,
								eastLon,
								southLat,
								northLat))))) {
			// the expected outcome is:
			// band 1,2,3,4,5,6 has every value set correctly, band 0 has every
			// even row set correctly and every odd row should be NaN, and band
			// 7 has the upper quadrant as NaN and the rest set
			final GridCoverage coverage = (GridCoverage) it.next();
			final Raster raster = coverage.getRenderedImage().getData();

			Assert.assertEquals(
					tileSize,
					raster.getWidth());
			Assert.assertEquals(
					tileSize,
					raster.getHeight());
			for (int x = 0; x < tileSize; x++) {
				for (int y = 0; y < tileSize; y++) {
					for (int b = 0; b < numBands; b++) {
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=" + b,
								expectedValue.getExpectedValue(
										x,
										y,
										b,
										numRasters,
										tileSize),
								raster.getSampleDouble(
										x,
										y,
										b),
								DOUBLE_TOLERANCE);

					}
				}
			}

			// there should be exactly one
			Assert.assertFalse(it.hasNext());
		}
	}

	private static interface ExpectedValue
	{
		public double getExpectedValue(
				int x,
				int y,
				int b,
				int numRasters,
				int tileSize );
	}

	private static class SummingExpectedValue implements
			ExpectedValue
	{
		@Override
		public double getExpectedValue(
				final int x,
				final int y,
				final int b,
				final int numRasters,
				final int tileSize ) {
			double sum = 0;
			for (int r = 0; r < numRasters; r++) {
				sum += getValue(
						x,
						y,
						b,
						r,
						tileSize);
			}
			return sum;
		}
	}

	private static class SumAndAveragingExpectedValue implements
			ExpectedValue
	{
		@Override
		public double getExpectedValue(
				final int x,
				final int y,
				final int b,
				final int numRasters,
				final int tileSize ) {
			double sum = 0;
			final boolean isSum = ((b % 2) == 0);

			for (int r = 0; r < numRasters; r++) {
				sum += getValue(
						x,
						y,
						isSum ? b : b - 1,
						r,
						tileSize);
			}
			if (isSum) {
				return sum;
			}
			else {
				return sum / numRasters;
			}
		}
	}

	/**
	 * this will sum up every band
	 */
	public static class SummingMergeStrategy extends
			SimpleAbstractMergeStrategy<Persistable>
	{

		protected SummingMergeStrategy() {
			super();
		}

		@Override
		protected double getSample(
				final int x,
				final int y,
				final int b,
				final double thisSample,
				final double nextSample ) {
			return thisSample + nextSample;
		}

	}

	/**
	 * this will sum up every even band and place the average of the previous
	 * band in each odd band
	 */
	public static class SumAndAveragingMergeStrategy implements
			RasterTileMergeStrategy<MergeCounter>
	{

		protected SumAndAveragingMergeStrategy() {
			super();
		}

		@Override
		public void merge(
				final RasterTile<MergeCounter> thisTile,
				final RasterTile<MergeCounter> nextTile,
				final SampleModel sampleModel ) {
			if (nextTile instanceof MergeableRasterTile) {
				final WritableRaster nextRaster = Raster.createWritableRaster(
						sampleModel,
						nextTile.getDataBuffer(),
						null);
				final WritableRaster thisRaster = Raster.createWritableRaster(
						sampleModel,
						thisTile.getDataBuffer(),
						null);
				final MergeCounter mergeCounter = thisTile.getMetadata();
				// we're merging, this is the incremented new number of merges
				final int newNumMerges = mergeCounter.getNumMerges() + 1;

				// we've merged 1 more tile than the total number of merges (ie.
				// if we've performed 1 merge, we've seen 2 tiles)
				final int totalTiles = newNumMerges + 1;
				final int maxX = nextRaster.getMinX() + nextRaster.getWidth();
				final int maxY = nextRaster.getMinY() + nextRaster.getHeight();
				for (int x = nextRaster.getMinX(); x < maxX; x++) {
					for (int y = nextRaster.getMinY(); y < maxY; y++) {
						for (int b = 0; (b + 1) < nextRaster.getNumBands(); b += 2) {
							final double thisSample = thisRaster.getSampleDouble(
									x,
									y,
									b);
							final double nextSample = nextRaster.getSampleDouble(
									x,
									y,
									b);
							final double sum = thisSample + nextSample;
							final double average = sum / totalTiles;
							thisRaster.setSample(
									x,
									y,
									b,
									sum);
							thisRaster.setSample(
									x,
									y,
									b + 1,
									average);
						}
					}
				}
				thisTile.setMetadata(new MergeCounter(
						newNumMerges));
			}
		}

		@Override
		public MergeCounter getMetadata(
				final GridCoverage tileGridCoverage,
				final RasterDataAdapter dataAdapter ) {
			// initial merge counter
			return new MergeCounter();
		}

		@Override
		public byte[] toBinary() {
			return new byte[] {};
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}

	}

	public static class MergeCounter implements
			Persistable
	{
		private int mergeCounter = 0;

		protected MergeCounter() {}

		protected MergeCounter(
				final int mergeCounter ) {
			this.mergeCounter = mergeCounter;
		}

		public int getNumMerges() {
			return mergeCounter;
		}

		@Override
		public byte[] toBinary() {
			final ByteBuffer buf = ByteBuffer.allocate(12);
			buf.putInt(mergeCounter);
			return buf.array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			final ByteBuffer buf = ByteBuffer.wrap(bytes);
			mergeCounter = buf.getInt();
		}
	}
}
