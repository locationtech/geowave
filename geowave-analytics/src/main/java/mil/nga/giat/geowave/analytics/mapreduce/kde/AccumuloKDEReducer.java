package mil.nga.giat.geowave.analytics.mapreduce.kde;

import java.awt.image.WritableRaster;
import java.io.IOException;

import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.raster.RasterUtils;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.opengis.coverage.grid.GridCoverage;

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

	public static final int NUM_BANDS = 3;
	private static final String[] NAME_PER_BAND = new String[] {
		"Weight",
		"Normalized",
		"Percentile"
	};

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
	private int tileSize;

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
				final WritableRaster raster = RasterUtils.createRasterTypeDouble(
						NUM_BANDS,
						tileSize);
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

				context.write(
						new GeoWaveOutputKey(
								new ByteArrayId(
										coverageName),
								new ByteArrayId(
										IndexType.SPATIAL_RASTER.getDefaultId())),
						RasterUtils.createCoverageTypeDouble(
								coverageName,
								tileInfo.tileWestLon,
								tileInfo.tileEastLon,
								tileInfo.tileSouthLat,
								tileInfo.tileNorthLat,
								new double[] {
									0,
									0,
									0
								},
								new double[] {
									max,
									1,
									1
								},
								NAME_PER_BAND,
								raster));
			}
		}
	}

	private TileInfo fromCellIndexToTileInfo(
			final long index ) {
		final int xPost = (int) Math.floor(index / numYPosts);
		final int yPost = (int) (index % numYPosts);
		final int xTile = xPost / tileSize;
		final int yTile = yPost / tileSize;
		final int x = (xPost % tileSize);
		final int y = (yPost % tileSize);
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
				KDEJobRunner.COVERAGE_NAME_KEY,
				"");
		tileSize = context.getConfiguration().getInt(
				KDEJobRunner.TILE_SIZE_KEY,
				1);
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
		numXTiles = (int) Math.ceil((double) numXPosts / (double) tileSize);
		numYTiles = (int) Math.ceil((double) numYPosts / (double) tileSize);
		totalKeys = context.getConfiguration().getLong(
				"Entries per level.level" + level,
				10);
	}
}
