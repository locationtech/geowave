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
package mil.nga.giat.geowave.analytic.mapreduce.kde.compare;

import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.vecmath.Point2d;

import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.analytic.mapreduce.kde.KDEJobRunner;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.opengis.coverage.grid.GridCoverage;

public class ComparisonAccumuloStatsReducer extends
		Reducer<ComparisonCellData, LongWritable, GeoWaveOutputKey, GridCoverage>
{
	public static final int NUM_BANDS = 4;
	protected static final String[] NAME_PER_BAND = new String[] {
		"Summer",
		"Winter",
		"Combined",
		"Combined Percentile"
	};
	protected static final double[] MINS_PER_BAND = new double[] {
		0,
		0,
		-1,
		0
	};
	protected static final double[] MAXES_PER_BAND = new double[] {
		1,
		1,
		1,
		1
	};
	private static final int TILE_SIZE = 1;
	private long totalKeys = 0;
	private long currentKey;

	private int minLevels;
	private int maxLevels;
	private int numLevels;
	private int level;
	private int numXPosts;
	private int numYPosts;
	private String coverageName;
	protected List<ByteArrayId> indexList;

	@Override
	protected void reduce(
			final ComparisonCellData key,
			final Iterable<LongWritable> values,
			final Context context )
			throws IOException,
			InterruptedException {
		// for consistency give all cells with matching weight the same
		// percentile
		final double percentile = (currentKey + 1.0) / totalKeys;
		// calculate weights for this key
		for (final LongWritable v : values) {
			final long cellIndex = v.get() / numLevels;
			final Point2d[] bbox = fromIndexToLL_UR(cellIndex);
			final WritableRaster raster = RasterUtils.createRasterTypeDouble(
					NUM_BANDS,
					TILE_SIZE);
			raster.setSample(
					0,
					0,
					0,
					key.getSummerPercentile());
			raster.setSample(
					0,
					0,
					1,
					key.getWinterPercentile());
			raster.setSample(
					0,
					0,
					2,
					key.getCombinedPercentile());
			raster.setSample(
					0,
					0,
					3,
					percentile);

			context.write(
					new GeoWaveOutputKey(
							new ByteArrayId(
									coverageName),
							indexList),
					RasterUtils.createCoverageTypeDouble(
							coverageName,
							bbox[0].x,
							bbox[1].x,
							bbox[0].y,
							bbox[1].y,
							MINS_PER_BAND,
							MAXES_PER_BAND,
							NAME_PER_BAND,
							raster));
			currentKey++;
		}
	}

	private Point2d[] fromIndexToLL_UR(
			final long index ) {
		final double llLon = ((Math.floor(index / (double) numYPosts) * 360.0) / numXPosts) - 180.0;
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
		coverageName = context.getConfiguration().get(
				KDEJobRunner.COVERAGE_NAME_KEY,
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
