package mil.nga.giat.geowave.analytics.mapreduce.kde;

import java.awt.Transparency;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.HashMap;

import javax.media.jai.RasterFactory;

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

import com.sun.media.imageioimpl.common.BogusColorSpace;

public class AccumuloKDEReducer extends
		Reducer<DoubleWritable, LongWritable, GeoWaveOutputKey, GridCoverage>
{
	private static final class TileInfo
	{
		private final double tileWestLon;
		private final double tileEastLon;
		private final double tileSouthLat;
		private final double tileNorthLat;
		private final int x;
		private final int y;

		public TileInfo(
				final double tileWestLon,
				final double tileEastLon,
				final double tileSouthLat,
				final double tileNorthLat,
				final int x,
				final int y ) {
			this.tileWestLon = tileWestLon;
			this.tileEastLon = tileEastLon;
			this.tileSouthLat = tileSouthLat;
			this.tileNorthLat = tileNorthLat;
			this.x = x;
			this.y = y;
		}
	}

	private final static Logger LOGGER = Logger.getLogger(AccumuloKDEReducer.class);
	public static final String COVERAGE_NAME_KEY = "COVERAGE_NAME";
	private static final int TILE_SIZE = 4;
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
	private int numXTiles;
	private int numYTiles;
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
				final TileInfo tileInfo = fromCellIndexToTileInfo(cellIndex);
				final WritableRaster raster = createRaster(NUM_BANDS);
				raster.setSample(
						tileInfo.x,
						tileInfo.y,
						0,
						key.get());
				raster.setSample(
						tileInfo.x,
						tileInfo.y,
						1,
						normalizedValue);
				inc += (1.0 / totalKeys);
				// the inc represents the percentile that this cell falls in
				raster.setSample(
						tileInfo.x,
						tileInfo.y,
						2,
						inc);
				Envelope mapExtent;
				try {
					mapExtent = new ReferencedEnvelope(
							tileInfo.tileWestLon,
							tileInfo.tileEastLon,
							tileInfo.tileSouthLat,
							tileInfo.tileNorthLat,
							GeoWaveGTRasterFormat.DEFAULT_CRS);
				}
				catch (final IllegalArgumentException e) {
					LOGGER.warn(
							"Unable to calculate transformation for grid coordinates on read",
							e);
					mapExtent = new Envelope2D(
							new DirectPosition2D(
									tileInfo.tileWestLon,
									tileInfo.tileSouthLat),
							new DirectPosition2D(
									tileInfo.tileEastLon,
									tileInfo.tileNorthLat));
				}

				final GridCoverageFactory gcf = CoverageFactoryFinder.getGridCoverageFactory(null);
				context.write(
						new GeoWaveOutputKey(
								new ByteArrayId(
										coverageName),
								new ByteArrayId(
										IndexType.SPATIAL_RASTER.getDefaultId())),
						gcf.create(
								coverageName,
								raster,
								mapExtent));
			}
		}
	}

	private TileInfo fromCellIndexToTileInfo(
			final long index ) {
		final int xPost = (int) Math.floor(index / numYPosts);
		final int yPost = (int) (index % numYPosts);
		final int xTile = xPost / TILE_SIZE;
		final int yTile = yPost / TILE_SIZE;
		final int x = (xPost % TILE_SIZE);
		final int y = (yPost % TILE_SIZE);
		final double tileWestLon = ((xTile * 360.0) / numXTiles) - 180.0;
		final double tileSouthLat = ((yTile * 180.0) / numYTiles) - 90.0;
		final double tileEastLon = tileWestLon + (360.0 / numXTiles);
		final double tileNorthLat = tileSouthLat + (180.0 / numYTiles);
		return new TileInfo(
				tileWestLon,
				tileEastLon,
				tileSouthLat,
				tileNorthLat,
				x,
				y);
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
		coverageName = context.getConfiguration().get(
				COVERAGE_NAME_KEY,
				"");
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
		numXTiles = (int) Math.ceil((double) numXPosts / (double) TILE_SIZE);
		numYTiles = (int) Math.ceil((double) numYPosts / (double) TILE_SIZE);
		totalKeys = context.getConfiguration().getLong(
				"Entries per level.level" + level,
				10);
	}
	
	public static WritableRaster createRaster(
			final int numBands ) {
		return RasterFactory.createBandedRaster(
				DataBuffer.TYPE_DOUBLE,
				TILE_SIZE,
				TILE_SIZE,
				numBands,
				null);
	}

	public static RasterDataAdapter getDataAdapter(
			final String coverageName,
			final int numBands ) {
		final double[][] noDataValuesPerBand = new double[numBands][];
		final double[] backgroundValuesPerBand = new double[numBands];
		final int[] bitsPerSample = new int[numBands];
		for (int i = 0; i < numBands; i++) {
			noDataValuesPerBand[i] = new double[] {
				Double.valueOf(Double.NaN)
			};
			backgroundValuesPerBand[i] = Double.valueOf(Double.NaN);
			bitsPerSample[i] = DataBuffer.getDataTypeSize(DataBuffer.TYPE_DOUBLE);
		}
		final SampleModel sampleModel = AccumuloKDEReducer.createRaster(
				numBands).getSampleModel();
		return new RasterDataAdapter(
				coverageName,
				sampleModel,
				new ComponentColorModel(
						new BogusColorSpace(
								numBands),
						bitsPerSample,
						false,
						false,
						Transparency.OPAQUE,
						DataBuffer.TYPE_DOUBLE),
				new HashMap<String, String>(),
				TILE_SIZE,
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
