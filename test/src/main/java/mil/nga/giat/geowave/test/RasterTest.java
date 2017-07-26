package mil.nga.giat.geowave.test;

import java.awt.Rectangle;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import javax.imageio.ImageIO;
import javax.media.jai.Interpolation;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.GeneralEnvelope;

import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveGTRasterFormat;
import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveRasterConfig;
import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveRasterReader;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStoreImpl;

public class RasterTest
{
	private static final double WEST = 113.09898918196271;
	private static final double EAST = 115.46527695561738;
	private static final double NORTH = -21.430488744575108;
	private static final double SOUTH = -27.522252769938202;

	public static void main(
			String[] args )
			throws IOException {
		HBaseStoreTestEnvironment.getInstance().zookeeper = "127.0.0.1:22010";
		DataStorePluginOptions dataStoreOptions = HBaseStoreTestEnvironment.getInstance().getDataStoreOptions(
				new GeoWaveTestStoreImpl(
						"",
						new GeoWaveTestStore.GeoWaveStoreType[] {
							GeoWaveStoreType.HBASE
						},
						new String[] {},
						GeoWaveTestStore.class));
		// AccumuloStoreTestEnvironment.getInstance().accumuloInstance =
		// "miniInstance";
		// AccumuloStoreTestEnvironment.getInstance().zookeeper =
		// "127.0.0.1:22010";
		// AccumuloStoreTestEnvironment.getInstance().accumuloPassword =
		// "Ge0wave";
		// AccumuloStoreTestEnvironment.getInstance().accumuloUser = "root";
		// DataStorePluginOptions dataStoreOptions =
		// AccumuloStoreTestEnvironment.getInstance().getDataStoreOptions(new
		// GeoWaveTestStoreImpl("", new
		// GeoWaveTestStore.GeoWaveStoreType[]{GeoWaveStoreType.ACCUMULO}, new
		// String[]{}, GeoWaveTestStore.class));
		final Map<String, String> options = dataStoreOptions.getOptionsAsMap();

		final StringBuilder str = new StringBuilder(
				"equalizeHistogramOverride=false;interpolationOverride=").append(Interpolation.INTERP_NEAREST);

		for (final Entry<String, String> entry : options.entrySet()) {
			if (!entry.getKey().equals(
					StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION)) {
				str.append(
						";").append(
						entry.getKey()).append(
						"=").append(
						entry.getValue());
			}
		}
		final GeneralEnvelope queryEnvelope = new GeneralEnvelope(
				new double[] {
					WEST,
					SOUTH
				},
				new double[] {
					EAST,
					NORTH
				});
		queryEnvelope.setCoordinateReferenceSystem(GeoWaveGTRasterFormat.DEFAULT_CRS);

		final GeoWaveRasterReader reader = new GeoWaveRasterReader(
				GeoWaveRasterConfig.readFromConfigParams(str.toString()));
		GridCoverage2D gridCoverage = null;
		long time = System.currentTimeMillis();
		for (int i = 0; i < 10; i++) {
			gridCoverage = reader.renderGridCoverage(
					"australia_shore_pan",
					new Rectangle(
							0,
							0,
							512,
							512),
					queryEnvelope,
					null,
					null,
					null);
		}
		System.err.println("elapsed time: " + (System.currentTimeMillis() - time));
		final RenderedImage result = gridCoverage.getRenderedImage();
		ImageIO.write(
				result,
				"png",
				new File(
						"C:\\Temp\\test.png"));
	}
}
