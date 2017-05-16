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
package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.CentroidPairing;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.clustering.exception.MatchingCentroidNotFoundException;
import mil.nga.giat.geowave.analytic.kmeans.AssociationNotification;
import mil.nga.giat.geowave.analytic.mapreduce.CountofDoubleWritable;
import mil.nga.giat.geowave.analytic.mapreduce.GroupIDText;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.mapreduce.GeoWaveWritableInputMapper;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Update the SINGLE cost of the clustering as a measure of distance from all
 * points to their closest center.
 * 
 * As an FYI: During the clustering algorithm, the cost should be monotonic
 * decreasing.
 * 
 * @formatter:off
 * 
 *                Context configuration parameters include:
 * 
 *                "UpdateCentroidCostMapReduce.Common.DistanceFunctionClass" ->
 *                Used to determine distance to centroid
 * 
 *                "UpdateCentroidCostMapReduce.Centroid.WrapperFactoryClass" ->
 *                {@link AnalyticItemWrapperFactory} to extract wrap spatial
 *                objects with Centroid management functions
 * 
 * @see CentroidManagerGeoWave
 * 
 * @formatter:on
 * 
 */

public class UpdateCentroidCostMapReduce
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(UpdateCentroidCostMapReduce.class);

	public static class UpdateCentroidCostMap extends
			GeoWaveWritableInputMapper<GroupIDText, CountofDoubleWritable>
	{
		private NestedGroupCentroidAssignment<Object> nestedGroupCentroidAssigner;
		private final CountofDoubleWritable dw = new CountofDoubleWritable();
		protected final GroupIDText outputWritable = new GroupIDText();
		protected AnalyticItemWrapperFactory<Object> itemWrapperFactory;

		private final AssociationNotification<Object> centroidAssociationFn = new AssociationNotification<Object>() {
			@Override
			public void notify(
					final CentroidPairing<Object> pairing ) {
				outputWritable.set(
						pairing.getCentroid().getGroupID(),
						pairing.getCentroid().getID());
			}
		};

		@Override
		protected void mapNativeValue(
				final GeoWaveInputKey key,
				final Object value,
				final Mapper<GeoWaveInputKey, ObjectWritable, GroupIDText, CountofDoubleWritable>.Context context )
				throws IOException,
				InterruptedException {
			final AnalyticItemWrapper<Object> wrappedItem = itemWrapperFactory.create(value);
			dw.set(
					nestedGroupCentroidAssigner.findCentroidForLevel(
							wrappedItem,
							centroidAssociationFn),
					1.0);

			context.write(
					outputWritable,
					dw);
		}

		@Override
		protected void setup(
				final Mapper<GeoWaveInputKey, ObjectWritable, GroupIDText, CountofDoubleWritable>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);

			final ScopedJobConfiguration config = new ScopedJobConfiguration(
					context.getConfiguration(),
					UpdateCentroidCostMapReduce.class,
					UpdateCentroidCostMapReduce.LOGGER);

			try {
				nestedGroupCentroidAssigner = new NestedGroupCentroidAssignment<Object>(
						context,
						UpdateCentroidCostMapReduce.class,
						UpdateCentroidCostMapReduce.LOGGER);

			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}

			try {
				itemWrapperFactory = config.getInstance(
						CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
						AnalyticItemWrapperFactory.class,
						SimpleFeatureItemWrapperFactory.class);

				itemWrapperFactory.initialize(
						context,
						UpdateCentroidCostMapReduce.class,
						UpdateCentroidCostMapReduce.LOGGER);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}
		}
	}

	public static class UpdateCentroidCostCombiner extends
			Reducer<GroupIDText, CountofDoubleWritable, GroupIDText, CountofDoubleWritable>
	{
		final CountofDoubleWritable outputValue = new CountofDoubleWritable();

		@Override
		public void reduce(
				final GroupIDText key,
				final Iterable<CountofDoubleWritable> values,
				final Reducer<GroupIDText, CountofDoubleWritable, GroupIDText, CountofDoubleWritable>.Context context )
				throws IOException,
				InterruptedException {

			double expectation = 0;
			double ptCount = 0;
			for (final CountofDoubleWritable value : values) {
				expectation += value.getValue();
				ptCount += value.getCount();
			}
			outputValue.set(
					expectation,
					ptCount);
			context.write(
					key,
					outputValue);
		}
	}

	public static class UpdateCentroidCostReducer extends
			Reducer<GroupIDText, CountofDoubleWritable, GeoWaveOutputKey, Object>
	{

		private CentroidManager<Object> centroidManager;
		private List<ByteArrayId> indexIds;

		@Override
		protected void reduce(
				final GroupIDText key,
				final Iterable<CountofDoubleWritable> values,
				final Reducer<GroupIDText, CountofDoubleWritable, GeoWaveOutputKey, Object>.Context context )
				throws IOException,
				InterruptedException {

			final String id = key.getID();
			final String groupID = key.getGroupID();

			double sum = 0.0;
			double count = 0;
			for (final CountofDoubleWritable next : values) {
				sum += next.getValue();
				count += next.getCount();
			}

			AnalyticItemWrapper<Object> centroid;
			try {
				centroid = getFeatureForCentroid(
						id,
						groupID);
			}
			catch (final MatchingCentroidNotFoundException e) {
				LOGGER.error(
						"Unable to get centroid " + id + " for group " + groupID,
						e);
				return;
			}

			centroid.setCost(sum);
			centroid.resetAssociatonCount();
			centroid.incrementAssociationCount((long) count);

			UpdateCentroidCostMapReduce.LOGGER.info("Update centroid " + centroid.toString());
			context.write(
					new GeoWaveOutputKey(
							centroidManager.getDataTypeId(),
							indexIds),
					centroid.getWrappedItem());
		}

		private AnalyticItemWrapper<Object> getFeatureForCentroid(
				final String id,
				final String groupID )
				throws IOException,
				MatchingCentroidNotFoundException {
			return centroidManager.getCentroidById(
					id,
					groupID);
		}

		@Override
		protected void setup(
				final Reducer<GroupIDText, CountofDoubleWritable, GeoWaveOutputKey, Object>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);

			try {
				centroidManager = new CentroidManagerGeoWave<Object>(

						context,
						UpdateCentroidCostMapReduce.class,
						UpdateCentroidCostMapReduce.LOGGER);
				indexIds = new ArrayList<ByteArrayId>();
				indexIds.add(centroidManager.getIndexId());
			}
			catch (final Exception e) {
				UpdateCentroidCostMapReduce.LOGGER.warn(
						"Unable to initialize centroid manager",
						e);
				throw new IOException(
						"Unable to initialize centroid manager");
			}
		}
	}

}
