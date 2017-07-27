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
import java.util.List;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.CentroidPairing;
import mil.nga.giat.geowave.analytic.clustering.DistortionGroupManagement;
import mil.nga.giat.geowave.analytic.clustering.DistortionGroupManagement.DistortionDataAdapter;
import mil.nga.giat.geowave.analytic.clustering.DistortionGroupManagement.DistortionEntry;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.extract.CentroidExtractor;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytic.kmeans.AssociationNotification;
import mil.nga.giat.geowave.analytic.mapreduce.CountofDoubleWritable;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.JumpParameters;
import mil.nga.giat.geowave.mapreduce.GeoWaveWritableInputMapper;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Point;

/**
 * Calculate the distortation.
 * <p/>
 * See Catherine A. Sugar and Gareth M. James (2003).
 * "Finding the number of clusters in a data set: An information theoretic approach"
 * Journal of the American Statistical Association 98 (January): 750–763
 * 
 * @formatter:off Context configuration parameters include:
 *                <p/>
 *                "KMeansDistortionMapReduce.Common.DistanceFunctionClass" ->
 *                {@link mil.nga.giat.geowave.analytic.distance.DistanceFn} used
 *                to determine distance to centroid
 *                <p/>
 *                "KMeansDistortionMapReduce.Centroid.WrapperFactoryClass" ->
 *                {@link AnalyticItemWrapperFactory} to extract wrap spatial
 *                objects with Centroid management functions
 *                <p/>
 *                "KMeansDistortionMapReduce.Centroid.ExtractorClass" ->
 *                {@link mil.nga.giat.geowave.analytic.extract.CentroidExtractor}
 *                <p/>
 *                "KMeansDistortionMapReduce.Jump.CountOfCentroids" -> May be
 *                different from actual.
 * @formatter:on
 * @see CentroidManagerGeoWave
 */
public class KMeansDistortionMapReduce
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(KMeansDistortionMapReduce.class);

	public static class KMeansDistortionMapper extends
			GeoWaveWritableInputMapper<Text, CountofDoubleWritable>
	{

		private NestedGroupCentroidAssignment<Object> nestedGroupCentroidAssigner;
		private final Text outputKeyWritable = new Text(
				"1");
		private final CountofDoubleWritable outputValWritable = new CountofDoubleWritable();
		private CentroidExtractor<Object> centroidExtractor;
		private AnalyticItemWrapperFactory<Object> itemWrapperFactory;

		AssociationNotification<Object> centroidAssociationFn = new AssociationNotification<Object>() {
			@Override
			public void notify(
					final CentroidPairing<Object> pairing ) {
				outputKeyWritable.set(pairing.getCentroid().getGroupID());
				final double extraFromItem[] = pairing.getPairedItem().getDimensionValues();
				final double extraCentroid[] = pairing.getCentroid().getDimensionValues();
				final Point p = centroidExtractor.getCentroid(pairing.getPairedItem().getWrappedItem());

				final Point centroid = centroidExtractor.getCentroid(pairing.getCentroid().getWrappedItem());

				// calculate error for dp
				// using identity matrix for the common covariance, therefore
				// E[(p - c)^-1 * cov * (p - c)] => (px - cx)^2 + (py - cy)^2
				double expectation = 0.0;
				for (int i = 0; i < extraCentroid.length; i++) {
					expectation += Math.pow(
							extraFromItem[i] - extraCentroid[i],
							2);
				}
				expectation += (Math.pow(
						p.getCoordinate().x - centroid.getCoordinate().x,
						2) + Math.pow(
						p.getCoordinate().y - centroid.getCoordinate().y,
						2));
				// + Math.pow(
				// p.getCoordinate().z - centroid.getCoordinate().z,
				// 2));
				outputValWritable.set(
						expectation,
						1);
			}
		};

		@Override
		protected void mapNativeValue(
				final GeoWaveInputKey key,
				final Object value,
				final org.apache.hadoop.mapreduce.Mapper<GeoWaveInputKey, ObjectWritable, Text, CountofDoubleWritable>.Context context )
				throws IOException,
				InterruptedException {
			nestedGroupCentroidAssigner.findCentroidForLevel(
					itemWrapperFactory.create(value),
					centroidAssociationFn);
			context.write(
					outputKeyWritable,
					outputValWritable);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(
				final Mapper<GeoWaveInputKey, ObjectWritable, Text, CountofDoubleWritable>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);
			final ScopedJobConfiguration config = new ScopedJobConfiguration(
					context.getConfiguration(),
					KMeansDistortionMapReduce.class,
					KMeansDistortionMapReduce.LOGGER);

			try {
				nestedGroupCentroidAssigner = new NestedGroupCentroidAssignment<Object>(

						context,
						KMeansDistortionMapReduce.class,
						KMeansDistortionMapReduce.LOGGER);
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
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}
		}
	}

	public static class KMeansDistorationCombiner extends
			Reducer<Text, CountofDoubleWritable, Text, CountofDoubleWritable>
	{
		final CountofDoubleWritable outputValue = new CountofDoubleWritable();

		@Override
		public void reduce(
				final Text key,
				final Iterable<CountofDoubleWritable> values,
				final Reducer<Text, CountofDoubleWritable, Text, CountofDoubleWritable>.Context context )
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

	public static class KMeansDistortionReduce extends
			Reducer<Text, CountofDoubleWritable, GeoWaveOutputKey, DistortionEntry>
	{
		private Integer expectedK = null;
		final protected Text output = new Text(
				"");
		private CentroidManagerGeoWave<Object> centroidManager;
		private String batchId;

		@Override
		public void reduce(
				final Text key,
				final Iterable<CountofDoubleWritable> values,
				final Reducer<Text, CountofDoubleWritable, GeoWaveOutputKey, DistortionEntry>.Context context )
				throws IOException,
				InterruptedException {
			double expectation = 0.0;
			final List<AnalyticItemWrapper<Object>> centroids = centroidManager.getCentroidsForGroup(key.toString());
			// it is possible that the number of items in a group are smaller
			// than the cluster
			final Integer kCount;
			if (expectedK == null) {
				kCount = centroids.size();
			}
			else {
				kCount = expectedK;
			}
			if (centroids.size() == 0) {
				return;
			}
			final double numDimesions = 2 + centroids.get(
					0).getExtraDimensions().length;

			double ptCount = 0;
			for (final CountofDoubleWritable value : values) {
				expectation += value.getValue();
				ptCount += value.getCount();
			}

			if (ptCount > 0) {
				expectation /= ptCount;

				final Double distortion = Math.pow(
						expectation / numDimesions,
						-(numDimesions / 2));

				final DistortionEntry entry = new DistortionEntry(
						key.toString(),
						batchId,
						kCount,
						distortion);

				context.write(
						new GeoWaveOutputKey(
								DistortionDataAdapter.ADAPTER_ID,
								DistortionGroupManagement.DISTORTIONS_INDEX_LIST),
						entry);
			}
		}

		@Override
		protected void setup(
				final Reducer<Text, CountofDoubleWritable, GeoWaveOutputKey, DistortionEntry>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);
			final ScopedJobConfiguration config = new ScopedJobConfiguration(
					context.getConfiguration(),
					KMeansDistortionMapReduce.class,
					KMeansDistortionMapReduce.LOGGER);

			final int k = config.getInt(
					JumpParameters.Jump.COUNT_OF_CENTROIDS,
					-1);
			if (k > 0) {
				expectedK = k;
			}

			try {
				centroidManager = new CentroidManagerGeoWave<Object>(
						context,
						KMeansDistortionMapReduce.class,
						KMeansDistortionMapReduce.LOGGER);
			}
			catch (final Exception e) {
				KMeansDistortionMapReduce.LOGGER.warn(
						"Unable to initialize centroid manager",
						e);
				throw new IOException(
						"Unable to initialize centroid manager",
						e);
			}

			batchId = config.getString(
					GlobalParameters.Global.PARENT_BATCH_ID,
					centroidManager.getBatchId());
		}
	}

}
