package mil.nga.giat.geowave.analytics.mapreduce.kde;

import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.HashMap;

import javax.media.jai.RasterFactory;
import javax.vecmath.Point2d;

import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.raster.plugin.GeoWaveGTRasterFormat;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.geometry.Envelope;

public class AccumuloKDEReducer extends
		Reducer<DoubleWritable, LongWritable, GeoWaveOutputKey, GridCoverage>
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloKDEReducer.class);
	public static final String STATS_NAME_KEY = "STATS_NAME";
	public static final int NUM_BANDS = 3;
	private long totalKeys = 0;
	private double max = -Double.MAX_VALUE;
	private double inc = 0;

	private int minLevels;
	private int maxLevels;
	private int numLevels;
	private int level;
	private int numXPosts;
	private int numYPosts;
	private String coverageName;

	@Override
	protected void reduce(
			final DoubleWritable key,
			final Iterable<LongWritable> values,
			final Context context )
			throws IOException,
			InterruptedException {
		if (key.get() < 0) {
			final double prevMax = -key.get();
			if (prevMax > max) {
				max = prevMax;
			}
		}
		else {
			final double value = key.get();
			final double normalizedValue = value / max;
			// calculate weights for this key
			for (final LongWritable v : values) {
				final long cellIndex = v.get() / numLevels;
				final Point2d[] bbox = fromIndexToLL_UR(cellIndex);
				final WritableRaster raster = createRaster(NUM_BANDS);
				raster.setSample(
						0,
						0,
						0,
						key.get());
				raster.setSample(
						0,
						0,
						1,
						normalizedValue);
				inc += (1.0 / totalKeys);
				// the inc represents the percentile that this cell falls in
				raster.setSample(
						0,
						0,
						2,
						inc);
				Envelope mapExtent;
				try {
					mapExtent = new ReferencedEnvelope(
							bbox[0].x,
							bbox[1].x,
							bbox[0].y,
							bbox[1].y,
							GeoWaveGTRasterFormat.DEFAULT_CRS);

				}
				catch (final IllegalArgumentException e) {
					LOGGER.warn(
							"Unable to calculate transformation for grid coordinates on read",
							e);
					mapExtent = new Envelope2D(
							new DirectPosition2D(
									bbox[0].x,
									bbox[0].y),
							new DirectPosition2D(
									bbox[0].x,
									bbox[0].y));
				}

				final GridCoverageFactory gcf = CoverageFactoryFinder.getGridCoverageFactory(null);
				context.write(
						new GeoWaveOutputKey(
								new ByteArrayId(
										coverageName),
								new ByteArrayId(
										IndexType.SPATIAL_VECTOR.getDefaultId())),
						gcf.create(
								coverageName,
								raster,
								mapExtent));
			}
		}
	}

	private Point2d[] fromIndexToLL_UR(
			final long index ) {
		final double llLon = ((Math.floor(index / numYPosts) * 360.0) / numXPosts) - 180.0;
		final double llLat = (((index % numYPosts) * 180.0) / numYPosts) - 90.0;
		final double urLon = llLon + (360.0 / numXPosts);
		final double urLat = llLat + (180.0 / numYPosts);
		return new Point2d[] {
			new Point2d(
					llLon,
					llLat),
			new Point2d(
					urLon,
					urLat)
		};
	}

	@Override
	protected void setup(
			final Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		minLevels = context.getConfiguration().getInt(
				KDEJobRunner.MIN_LEVEL_KEY,
				1);
		maxLevels = context.getConfiguration().getInt(
				KDEJobRunner.MAX_LEVEL_KEY,
				25);
		final String statsName = context.getConfiguration().get(
				STATS_NAME_KEY,
				"");
		coverageName = getCoverageName(statsName);
		numLevels = (maxLevels - minLevels) + 1;
		level = context.getConfiguration().getInt(
				"mapred.task.partition",
				0) + minLevels;
		numXPosts = (int) Math.pow(
				2,
				level + 1);
		numYPosts = (int) Math.pow(
				2,
				level);
		totalKeys = context.getConfiguration().getLong(
				"Entries per level.level" + level,
				10);
	}

	public static String getCoverageName(
			final String statsName ) {
		return "stats_" + statsName;
	}

	public static WritableRaster createRaster(
			final int numBands ) {
		return RasterFactory.createBandedRaster(
				DataBuffer.TYPE_DOUBLE,
				1,
				1,
				numBands,
				null);
	}

	public static RasterDataAdapter getDataAdapter(
			final String statsName,
			final int numBands ) {
		final String coverageName = AccumuloKDEReducer.getCoverageName(statsName);
		final double[][] noDataValuesPerBand = new double[numBands][];
		final double[] backgroundValuesPerBand = new double[numBands];
		for (int i = 0; i < numBands; i++) {
			noDataValuesPerBand[i] = new double[] {
				Double.valueOf(Double.NaN)
			};
			backgroundValuesPerBand[i] = Double.valueOf(Double.NaN);
		}
		final SampleModel sampleModel = AccumuloKDEReducer.createRaster(
				numBands).getSampleModel();
		return new RasterDataAdapter(
				coverageName,
				sampleModel,
				new ComponentColorModel(
						ColorSpace.getInstance(ColorSpace.CS_GRAY),
						new int[] {
							16
						},
						false,
						true,
						Transparency.OPAQUE,
						DataBuffer.TYPE_DOUBLE),
				new HashMap<String, String>(),
				1,
				noDataValuesPerBand,
				backgroundValuesPerBand,
				null,
				false,
				new NoDataMergeStrategy(
						new ByteArrayId(
								coverageName),
						sampleModel));
	}

}
