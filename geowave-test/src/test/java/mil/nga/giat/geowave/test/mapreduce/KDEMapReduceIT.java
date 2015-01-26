package mil.nga.giat.geowave.test.mapreduce;

import java.awt.Rectangle;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;

import javax.media.jai.Interpolation;

import junit.framework.Assert;
import mil.nga.giat.geowave.accumulo.util.ConnectorPool;
import mil.nga.giat.geowave.analytics.mapreduce.kde.KDEJobRunner;
import mil.nga.giat.geowave.raster.plugin.GeoWaveGTRasterFormat;
import mil.nga.giat.geowave.raster.plugin.GeoWaveRasterConfig;
import mil.nga.giat.geowave.raster.plugin.GeoWaveRasterReader;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.types.gpx.GpxUtils;

import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.util.ToolRunner;
import org.geotools.geometry.GeneralEnvelope;
import org.junit.Test;
import org.opengis.coverage.grid.GridCoverage;

public class KDEMapReduceIT extends
		MapReduceTestEnvironment
{
	private static final String TEST_COVERAGE_NAME_PREFIX = "TEST_COVERAGE";
	private static final int MAX_TILE_SIZE_POWER_OF_2 = 5;
	private static final int BASE_MIN_LEVEL = 14;
	private static final int BASE_MAX_LEVEL = 16;

	@Test
	public void testIngestAndQueryGeneralGpx()
			throws Exception {
		accumuloOperations.deleteAll();
		testIngest(
				IndexType.SPATIAL_VECTOR,
				GENERAL_GPX_INPUT_GPX_DIR);
		for (int i = 0; i <= MAX_TILE_SIZE_POWER_OF_2; i++) {
			final String tileSizeCoverageName = TEST_COVERAGE_NAME_PREFIX + i;
			ToolRunner.run(
					new KDEJobRunner(),
					new String[] {
						zookeeper,
						accumuloInstance,
						accumuloUser,
						accumuloPassword,
						TEST_NAMESPACE,
						GpxUtils.GPX_WAYPOINT_FEATURE,
						new Integer(
								BASE_MIN_LEVEL - i).toString(),
						new Integer(
								BASE_MAX_LEVEL - i).toString(),
						new Integer(
								MIN_INPUT_SPLITS).toString(),
						new Integer(
								MAX_INPUT_SPLITS).toString(),
						tileSizeCoverageName,
						hdfs,
						jobtracker,
						TEST_NAMESPACE,
						new Integer(
								(int) Math.pow(
										2,
										i)).toString()
					});
		}
		final GeoWaveRasterReader reader = new GeoWaveRasterReader(
				GeoWaveRasterConfig.createConfig(
						zookeeper,
						accumuloInstance,
						accumuloUser,
						accumuloPassword,
						TEST_NAMESPACE,
						false,
						Interpolation.INTERP_NEAREST));
		final GeneralEnvelope queryEnvelope = new GeneralEnvelope(
				new double[] {
					-71.12,
					42.38
				},
				new double[] {
					-71.07,
					42.4
				});
		queryEnvelope.setCoordinateReferenceSystem(GeoWaveGTRasterFormat.DEFAULT_CRS);
		final Raster[] rasters = new Raster[MAX_TILE_SIZE_POWER_OF_2 + 1];
		for (int i = 0; i < rasters.length; i++) {
			final String tileSizeCoverageName = TEST_COVERAGE_NAME_PREFIX + i;
			final GridCoverage gridCoverage = reader.renderGridCoverage(
					tileSizeCoverageName,
					new Rectangle(
							6,
							6),
					queryEnvelope,
					null,
					null);
			final RenderedImage image = gridCoverage.getRenderedImage();
			final Raster raster = image.getData();
			rasters[i] = raster;
		}
		final int[] counts = new int[rasters.length];
		for (int i = 0; i < counts.length; i++) {
			counts[i] = 0;
			for (int x = 0; x < rasters[i].getWidth(); x++) {
				for (int y = 0; y < rasters[i].getHeight(); y++) {
					for (int b = 0; b < rasters[i].getNumBands(); b++) {
						final double sample = rasters[i].getSampleDouble(
								x,
								y,
								b);
						if (!Double.isNaN(sample)) {
							counts[i]++;
						}
					}
				}
			}
		}
		// make sure all of the counts are the same before and after compaction
		for (int i = 1; i < counts.length; i++) {
			Assert.assertEquals(
					"The count of NaN values is different between the 1 pixel KDE and the 2^" + i + " pixel KDE",
					counts[0],
					counts[i]);
		}
		final Connector conn = ConnectorPool.getInstance().getConnector(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword);
		conn.tableOperations().compact(
				TEST_NAMESPACE + "_" + IndexType.SPATIAL_RASTER.createDefaultIndex().getId().getString(),
				null,
				null,
				true,
				true);

		for (int i = 0; i < rasters.length; i++) {
			final String tileSizeCoverageName = TEST_COVERAGE_NAME_PREFIX + i;
			final GridCoverage gridCoverage = reader.renderGridCoverage(
					tileSizeCoverageName,
					// note that this time we are also trying with a different
					// requested pixel dimension
					new Rectangle(
							128,
							128),
					queryEnvelope,
					null,
					null);
			final RenderedImage image = gridCoverage.getRenderedImage();
			final Raster raster = image.getData();
			rasters[i] = raster;
		}
		for (int i = 0; i < rasters.length; i++) {
			counts[i] = 0;
			for (int x = 0; x < rasters[i].getWidth(); x++) {
				for (int y = 0; y < rasters[i].getHeight(); y++) {
					for (int b = 0; b < rasters[i].getNumBands(); b++) {
						final double sample = rasters[i].getSampleDouble(
								x,
								y,
								b);
						if (!Double.isNaN(sample)) {
							counts[i]++;
						}
					}
				}
			}
		}

		for (int i = 1; i < counts.length; i++) {
			Assert.assertEquals(
					"The count of NaN values is different between the 1 pixel KDE and the 2^" + i + " pixle KDE",
					counts[0],
					counts[i]);
		}

	}
}
