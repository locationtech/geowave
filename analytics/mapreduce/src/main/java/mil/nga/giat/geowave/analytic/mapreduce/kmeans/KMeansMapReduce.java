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
import mil.nga.giat.geowave.analytic.GeoObjectDimensionValues;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.CentroidPairing;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.clustering.exception.MatchingCentroidNotFoundException;
import mil.nga.giat.geowave.analytic.extract.CentroidExtractor;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytic.kmeans.AssociationNotification;
import mil.nga.giat.geowave.analytic.mapreduce.GroupIDText;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.mapreduce.GeoWaveWritableInputMapper;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Point;

/**
 * K-Means mapper and reducer. Mapper determines the closest centroid for an
 * item in the item's assigned group. A group contains one or more centroids.
 * The dimensions for the item are sent to the reducer along with the closest
 * centroid ID.
 * <p/>
 * Reducer Outputs a new copy of a centroid with the geometry and other
 * dimensions updated towards their respective mean for the assigned items.
 * <p/>
 * Properties:
 * 
 * @formatter:off "KMeansMapReduce.Common.DistanceFunctionClass" - Used to
 *                determine distance to centroid
 *                <p/>
 *                "KMeansMapReduce.Centroid.ExtractorClass" - Used to extract a
 *                centroid point from an item geometry
 *                <p/>
 *                "KMeansMapReduce.Centroid.WrapperFactoryClass" -
 *                {@link AnalyticItemWrapperFactory} to extract wrap spatial
 *                objects with Centroid management function
 *                <p/>
 *                "KMeansMapReduce.Centroid.ZoomLevel" -> The current zoom level
 * @See CentroidManagerGeoWave
 * @formatter:on
 */
public class KMeansMapReduce
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(KMeansMapReduce.class);

	public static class KMeansMapper extends
			GeoWaveWritableInputMapper<GroupIDText, BytesWritable>
	{

		private NestedGroupCentroidAssignment<Object> nestedGroupCentroidAssigner;
		private final GroupIDText outputKeyWritable = new GroupIDText();
		private final BytesWritable outputValWritable = new BytesWritable();
		private final GeoObjectDimensionValues association = new GeoObjectDimensionValues();
		protected CentroidExtractor<Object> centroidExtractor;
		protected AnalyticItemWrapperFactory<Object> itemWrapperFactory;

		AssociationNotification<Object> centroidAssociationFn = new AssociationNotification<Object>() {
			@Override
			public void notify(
					final CentroidPairing<Object> pairing ) {
				outputKeyWritable.set(
						pairing.getCentroid().getGroupID(),
						pairing.getCentroid().getID());
				final double extra[] = pairing.getPairedItem().getDimensionValues();
				final Point p = centroidExtractor.getCentroid(pairing.getPairedItem().getWrappedItem());
				association.set(
						p.getCoordinate().x,
						p.getCoordinate().y,
						p.getCoordinate().z,
						extra,
						pairing.getDistance());
			}
		};

		@Override
		protected void mapNativeValue(
				final GeoWaveInputKey key,
				final Object value,
				final org.apache.hadoop.mapreduce.Mapper<GeoWaveInputKey, ObjectWritable, GroupIDText, BytesWritable>.Context context )
				throws IOException,
				InterruptedException {
			final AnalyticItemWrapper<Object> item = itemWrapperFactory.create(value);
			nestedGroupCentroidAssigner.findCentroidForLevel(
					item,
					centroidAssociationFn);
			final byte[] outData = association.toBinary();
			outputValWritable.set(
					outData,
					0,
					outData.length);
			context.write(
					outputKeyWritable,
					outputValWritable);
		}

		@Override
		protected void setup(
				final Mapper<GeoWaveInputKey, ObjectWritable, GroupIDText, BytesWritable>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);
			final ScopedJobConfiguration config = new ScopedJobConfiguration(
					context.getConfiguration(),
					KMeansMapReduce.class,
					KMeansMapReduce.LOGGER);

			try {
				nestedGroupCentroidAssigner = new NestedGroupCentroidAssignment<Object>(
						context,
						KMeansMapReduce.class,
						KMeansMapReduce.LOGGER);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}

			try {
				centroidExtractor = config.getInstance(
						CentroidParameters.Centroid.EXTRACTOR_CLASS,
						CentroidExtractor.class,
						SimpleFeatureCentroidExtractor.class);
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
						KMeansMapReduce.class,
						KMeansMapReduce.LOGGER);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}
		}
	}

	/**
	 * Optimization
	 */
	public static class KMeansCombiner extends
			Reducer<GroupIDText, BytesWritable, GroupIDText, BytesWritable>
	{
		private final GeoObjectDimensionValues geoObject = new GeoObjectDimensionValues();
		private final BytesWritable outputValWritable = new BytesWritable();

		@Override
		public void reduce(
				final GroupIDText key,
				final Iterable<BytesWritable> values,
				final Reducer<GroupIDText, BytesWritable, GroupIDText, BytesWritable>.Context context )
				throws IOException,
				InterruptedException {
			final GeoObjectDimensionValues totals = new GeoObjectDimensionValues();

			for (final BytesWritable value : values) {
				geoObject.fromBinary(value.getBytes());
				totals.add(geoObject);
			}
			final byte[] outData = totals.toBinary();
			outputValWritable.set(
					outData,
					0,
					outData.length);
			context.write(
					key,
					outputValWritable);
		}
	}

	public static class KMeansReduce extends
			Reducer<GroupIDText, BytesWritable, GeoWaveOutputKey, Object>
	{

		protected CentroidManager<Object> centroidManager;
		private final GeoObjectDimensionValues geoObject = new GeoObjectDimensionValues();
		private List<ByteArrayId> indexIds;

		@Override
		public void reduce(
				final GroupIDText key,
				final Iterable<BytesWritable> values,
				final Reducer<GroupIDText, BytesWritable, GeoWaveOutputKey, Object>.Context context )
				throws IOException,
				InterruptedException {
			final String centroidID = key.getID();
			final String groupID = key.getGroupID();
			final GeoObjectDimensionValues totals = new GeoObjectDimensionValues();

			for (final BytesWritable value : values) {
				geoObject.fromBinary(value.getBytes());
				totals.add(geoObject);
			}

			AnalyticItemWrapper<Object> centroid;
			try {
				centroid = getFeatureForCentroid(
						centroidID,
						groupID);
			}
			catch (final MatchingCentroidNotFoundException e) {
				LOGGER.error(
						"Unable to get centroid " + centroidID + " for group " + groupID,
						e);
				return;
			}

			// do not update the cost, because this cost is associated with the
			// centroid PRIOR to this update.
			// centroid.setCost(totals.distance);
			centroid.resetAssociatonCount();
			centroid.incrementAssociationCount(totals.getCount());

			final double ptCount = totals.getCount();
			// mean
			totals.x = totals.x / ptCount;
			totals.y = totals.y / ptCount;
			totals.z = totals.z / ptCount;

			final int s = centroid.getExtraDimensions().length;
			for (int i = 0; i < s; i++) {
				totals.values[i] = totals.values[i] / ptCount;
			}

			if (KMeansMapReduce.LOGGER.isTraceEnabled()) {
				KMeansMapReduce.LOGGER.trace(groupID + " contains " + centroidID);
			}

			final AnalyticItemWrapper<Object> nextCentroid = centroidManager.createNextCentroid(
					centroid.getWrappedItem(),
					groupID,
					new Coordinate(
							totals.x,
							totals.y,
							totals.z),
					centroid.getExtraDimensions(),
					totals.values);

			// new center
			context.write(
					new GeoWaveOutputKey(
							centroidManager.getDataTypeId(),
							indexIds),
					nextCentroid.getWrappedItem());

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
				final Reducer<GroupIDText, BytesWritable, GeoWaveOutputKey, Object>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);
			try {
				centroidManager = new CentroidManagerGeoWave<Object>(
						context,
						KMeansMapReduce.class,
						KMeansMapReduce.LOGGER);
				indexIds = new ArrayList<ByteArrayId>();
				indexIds.add(centroidManager.getIndexId());
			}
			catch (final Exception e) {
				throw new IOException(
						e);
			}

		}
	}

}
