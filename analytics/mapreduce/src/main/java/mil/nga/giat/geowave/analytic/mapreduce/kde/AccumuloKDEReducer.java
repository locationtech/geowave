/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.analytic.mapreduce.kde;

import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.opengis.coverage.grid.GridCoverage;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.analytic.mapreduce.kde.GaussianFilter.ValueRange;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

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

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			long temp;
			temp = Double.doubleToLongBits(tileEastLon);
			result = (prime * result) + (int) (temp ^ (temp >>> 32));
			temp = Double.doubleToLongBits(tileNorthLat);
			result = (prime * result) + (int) (temp ^ (temp >>> 32));
			temp = Double.doubleToLongBits(tileSouthLat);
			result = (prime * result) + (int) (temp ^ (temp >>> 32));
			temp = Double.doubleToLongBits(tileWestLon);
			result = (prime * result) + (int) (temp ^ (temp >>> 32));
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final TileInfo other = (TileInfo) obj;
			if (Double.doubleToLongBits(tileEastLon) != Double.doubleToLongBits(other.tileEastLon)) {
				return false;
			}
			if (Double.doubleToLongBits(tileNorthLat) != Double.doubleToLongBits(other.tileNorthLat)) {
				return false;
			}
			if (Double.doubleToLongBits(tileSouthLat) != Double.doubleToLongBits(other.tileSouthLat)) {
				return false;
			}
			if (Double.doubleToLongBits(tileWestLon) != Double.doubleToLongBits(other.tileWestLon)) {
				return false;
			}
			return true;
		}
	}

	public static final int NUM_BANDS = 3;
	protected static final String[] NAME_PER_BAND = new String[] {
		"Weight",
		"Normalized",
		"Percentile"
	};

	protected static final double[] MINS_PER_BAND = new double[] {
		0,
		0,
		0
	};
	protected static final double[] MAXES_PER_BAND = new double[] {
		Double.MAX_VALUE,
		1,
		1
	};
	private double max = -Double.MAX_VALUE;
	private long currentKey = 0;
	private long totalKeys;

	private int minLevels;
	private int maxLevels;
	private int numLevels;
	private int level;
	private int numYPosts;
	private int numXTiles;
	private int numYTiles;
	private String coverageName;
	protected List<ByteArrayId> indexList;
	protected ValueRange[] valueRangePerDimension;
	protected String crsCode;

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
			// for consistency give all cells with matching weight the same
			// percentile
			final double percentile = (currentKey + 1.0) / totalKeys;
			// calculate weights for this key
			for (final LongWritable v : values) {
				final long cellIndex = v.get() / numLevels;
				final TileInfo tileInfo = fromCellIndexToTileInfo(cellIndex);
				final WritableRaster raster = RasterUtils.createRasterTypeDouble(
						NUM_BANDS,
						KDEJobRunner.TILE_SIZE);
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

				raster.setSample(
						tileInfo.x,
						tileInfo.y,
						2,
						percentile);
				context.write(
						new GeoWaveOutputKey(
								new ByteArrayId(
										coverageName),
								indexList),
						RasterUtils.createCoverageTypeDouble(
								coverageName,
								tileInfo.tileWestLon,
								tileInfo.tileEastLon,
								tileInfo.tileSouthLat,
								tileInfo.tileNorthLat,
								MINS_PER_BAND,
								MAXES_PER_BAND,
								NAME_PER_BAND,
								raster,
								crsCode));
				currentKey++;
			}
		}
	}

	@SuppressFBWarnings(value = "INT_BAD_REM_BY_1", justification = "The calculation is appropriate if we ever want to vary to tile size.")
	private TileInfo fromCellIndexToTileInfo(
			final long index ) {
		final int xPost = (int) (index / numYPosts);
		final int yPost = (int) (index % numYPosts);
		final int xTile = xPost / KDEJobRunner.TILE_SIZE;
		final int yTile = yPost / KDEJobRunner.TILE_SIZE;
		final int x = (xPost % KDEJobRunner.TILE_SIZE);
		final int y = (yPost % KDEJobRunner.TILE_SIZE);
		final double xMin = valueRangePerDimension[0].getMin();
		final double xMax = valueRangePerDimension[0].getMax();
		final double yMin = valueRangePerDimension[1].getMin();
		final double yMax = valueRangePerDimension[1].getMax();
		final double crsWidth = xMax - xMin;
		final double crsHeight = yMax - yMin;
		final double tileWestLon = ((xTile * crsWidth) / numXTiles) + xMin;
		final double tileSouthLat = ((yTile * crsHeight) / numYTiles) + yMin;
		final double tileEastLon = tileWestLon + (crsWidth / numXTiles);
		final double tileNorthLat = tileSouthLat + (crsHeight / numYTiles);
		return new TileInfo(
				tileWestLon,
				tileEastLon,
				tileSouthLat,
				tileNorthLat,
				x,
				KDEJobRunner.TILE_SIZE - y - 1); // remember java rasters go
													// from 0 at the
		// top
		// to (height-1) at the bottom, so we have
		// to
		// inverse the y here which goes from bottom
		// to top
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
		valueRangePerDimension = new ValueRange[] {
			new ValueRange(
					context.getConfiguration().getDouble(
							KDEJobRunner.X_MIN_KEY,
							-180),
					context.getConfiguration().getDouble(
							KDEJobRunner.X_MAX_KEY,
							180)),
			new ValueRange(
					context.getConfiguration().getDouble(
							KDEJobRunner.Y_MIN_KEY,
							-90),
					context.getConfiguration().getDouble(
							KDEJobRunner.Y_MAX_KEY,
							90))
		};
		crsCode = context.getConfiguration().get(
				KDEJobRunner.OUTPUT_CRSCODE_KEY);

		numLevels = (maxLevels - minLevels) + 1;
		level = context.getConfiguration().getInt(
				"mapred.task.partition",
				0) + minLevels;
		numXTiles = (int) Math.pow(
				2,
				level + 1);
		numYTiles = (int) Math.pow(
				2,
				level);
		numYPosts = numYTiles * KDEJobRunner.TILE_SIZE;

		totalKeys = context.getConfiguration().getLong(
				"Entries per level.level" + level,
				10);
		final PrimaryIndex[] indices = JobContextIndexStore.getIndices(context);
		indexList = new ArrayList<ByteArrayId>();
		if ((indices != null) && (indices.length > 0)) {
			for (final PrimaryIndex index : indices) {
				indexList.add(index.getId());
			}

		}
		else {
			indexList.add(new SpatialDimensionalityTypeProvider.SpatialIndexBuilder().createIndex().getId());
		}
	}
}
