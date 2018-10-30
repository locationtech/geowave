/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.analytic.mapreduce.kmeans.runner;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.locationtech.geowave.analytic.AnalyticItemWrapperFactory;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.analytic.clustering.DistortionGroupManagement;
import org.locationtech.geowave.analytic.distance.FeatureCentroidDistanceFn;
import org.locationtech.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import org.locationtech.geowave.analytic.extract.SimpleFeatureGeometryExtractor;
import org.locationtech.geowave.analytic.mapreduce.MapReduceJobController;
import org.locationtech.geowave.analytic.mapreduce.MapReduceJobRunner;
import org.locationtech.geowave.analytic.mapreduce.clustering.runner.ClusteringRunner;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.ClusteringParameters;
import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.FormatConfiguration;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.analytic.param.JumpParameters;
import org.locationtech.geowave.analytic.param.MapReduceParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.SampleParameters;
import org.locationtech.geowave.analytic.param.StoreParameters;
import org.locationtech.geowave.analytic.param.StoreParameters.StoreParam;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The KMeans Jump algorithm
 * 
 * Catherine A. Sugar and Gareth M. James (2003).
 * "Finding the number of clusters in a data set: An information theoretic approach"
 * Journal of the American Statistical Association 98 (January): 750–763
 * 
 * @formatter:off Couple things to note:
 * 
 * 
 * @formatter:on
 * 
 */
public class KMeansJumpJobRunner extends
		MapReduceJobController implements
		ClusteringRunner
{
	final static Logger LOGGER = LoggerFactory.getLogger(KMeansJumpJobRunner.class);
	final KMeansDistortionJobRunner jumpRunner = new KMeansDistortionJobRunner();
	final KMeansParallelJobRunnerDelegate kmeansRunner = new KMeansParallelJobRunnerDelegate();

	private int currentZoomLevel = 1;

	public KMeansJumpJobRunner() {
		// defaults
		setZoomLevel(1);

		// child runners
		init(
				new MapReduceJobRunner[] {
					kmeansRunner,
					jumpRunner,
				},
				new PostOperationTask[] {
					DoNothingTask,
					DoNothingTask
				});
	}

	@Override
	public void setZoomLevel(
			final int zoomLevel ) {
		currentZoomLevel = zoomLevel;
		kmeansRunner.setZoomLevel(zoomLevel);
	}

	@Override
	public void setInputFormatConfiguration(
			final FormatConfiguration inputFormatConfiguration ) {
		jumpRunner.setInputFormatConfiguration(inputFormatConfiguration);
		kmeansRunner.setInputFormatConfiguration(inputFormatConfiguration);
	}

	@Override
	@SuppressWarnings("unchecked")
	public int run(
			final Configuration configuration,
			final PropertyManagement propertyManagement )
			throws Exception {

		propertyManagement.store(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				currentZoomLevel);

		propertyManagement.storeIfEmpty(
				GlobalParameters.Global.BATCH_ID,
				UUID.randomUUID().toString());

		propertyManagement.storeIfEmpty(
				CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);
		propertyManagement.storeIfEmpty(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);
		propertyManagement.storeIfEmpty(
				CentroidParameters.Centroid.EXTRACTOR_CLASS,
				SimpleFeatureCentroidExtractor.class);
		propertyManagement.storeIfEmpty(
				CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
				SimpleFeatureGeometryExtractor.class);

		propertyManagement.copy(
				CentroidParameters.Centroid.DATA_TYPE_ID,
				SampleParameters.Sample.DATA_TYPE_NAME);

		propertyManagement.copy(
				CentroidParameters.Centroid.INDEX_NAME,
				SampleParameters.Sample.INDEX_NAME);

		ClusteringUtils.createAdapter(propertyManagement);
		ClusteringUtils.createIndex(propertyManagement);

		final String currentBatchId = propertyManagement.getPropertyAsString(
				GlobalParameters.Global.BATCH_ID,
				UUID.randomUUID().toString());

		try {

			final NumericRange rangeOfIterations = propertyManagement.getPropertyAsRange(
					JumpParameters.Jump.RANGE_OF_CENTROIDS,
					new NumericRange(
							2,
							200));
			propertyManagement.store(
					GlobalParameters.Global.PARENT_BATCH_ID,
					currentBatchId);

			final DataStorePluginOptions dataStoreOptions = ((PersistableStore) propertyManagement
					.getProperty(StoreParam.INPUT_STORE)).getDataStoreOptions();

			final DistortionGroupManagement distortionGroupManagement = new DistortionGroupManagement(
					dataStoreOptions);

			for (int k = (int) Math.max(
					2,
					Math.round(rangeOfIterations.getMin())); k < Math.round(rangeOfIterations.getMax()); k++) {

				// regardless of the algorithm, the sample set is fixed in size
				propertyManagement.store(
						SampleParameters.Sample.MIN_SAMPLE_SIZE,
						k);
				propertyManagement.store(
						SampleParameters.Sample.MAX_SAMPLE_SIZE,
						k);
				propertyManagement.store(
						SampleParameters.Sample.SAMPLE_SIZE,
						k);

				jumpRunner.setCentroidsCount(k);
				jumpRunner.setDataStoreOptions(dataStoreOptions);
				final String iterationBatchId = currentBatchId + "_" + k;
				propertyManagement.store(
						GlobalParameters.Global.BATCH_ID,
						iterationBatchId);
				jumpRunner.setReducerCount(k);
				final int status = super.run(
						configuration,
						propertyManagement);
				if (status != 0) {
					return status;
				}
			}
			propertyManagement.store(
					GlobalParameters.Global.BATCH_ID,
					currentBatchId);

			@SuppressWarnings("rawtypes")
			final Class<AnalyticItemWrapperFactory> analyticItemWrapperFC = propertyManagement.getPropertyAsClass(
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					AnalyticItemWrapperFactory.class);

			/**
			 * Associate the batch id with the best set of groups so the caller
			 * can find the clusters for the given batch
			 */
			final int result = distortionGroupManagement.retainBestGroups(
					(AnalyticItemWrapperFactory<SimpleFeature>) analyticItemWrapperFC.newInstance(),
					propertyManagement.getPropertyAsString(CentroidParameters.Centroid.DATA_TYPE_ID),
					propertyManagement.getPropertyAsString(CentroidParameters.Centroid.INDEX_NAME),
					currentBatchId,
					currentZoomLevel);

			return result;
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Cannot create distortions",
					ex);
			return 1;
		}

	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
		params.addAll(kmeansRunner.singleSamplekmeansJobRunner.getParameters());
		params.addAll(kmeansRunner.parallelJobRunner.getParameters());
		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			JumpParameters.Jump.RANGE_OF_CENTROIDS,
			JumpParameters.Jump.KPLUSPLUS_MIN,
			ClusteringParameters.Clustering.MAX_REDUCER_COUNT,
			CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
			CentroidParameters.Centroid.INDEX_NAME,
			CentroidParameters.Centroid.DATA_TYPE_ID,
			CentroidParameters.Centroid.DATA_NAMESPACE_URI,
			CentroidParameters.Centroid.EXTRACTOR_CLASS,
			CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
			CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
			StoreParameters.StoreParam.INPUT_STORE,
			GlobalParameters.Global.BATCH_ID
		}));
		params.addAll(MapReduceParameters.getParameters());

		params.remove(CentroidParameters.Centroid.ZOOM_LEVEL);
		params.remove(SampleParameters.Sample.DATA_TYPE_NAME);
		params.remove(SampleParameters.Sample.INDEX_NAME);
		return params;
	}

	private static class KMeansParallelJobRunnerDelegate implements
			MapReduceJobRunner
	{
		final KMeansSingleSampleJobRunner<SimpleFeature> singleSamplekmeansJobRunner = new KMeansSingleSampleJobRunner<SimpleFeature>();
		final KMeansParallelJobRunner parallelJobRunner = new KMeansParallelJobRunner();

		@Override
		public int run(
				final Configuration config,
				final PropertyManagement runTimeProperties )
				throws Exception {
			final int k = runTimeProperties.getPropertyAsInt(
					SampleParameters.Sample.SAMPLE_SIZE,
					1);
			final int minkplusplus = runTimeProperties.getPropertyAsInt(
					JumpParameters.Jump.KPLUSPLUS_MIN,
					3);
			if (k >= minkplusplus) {
				return parallelJobRunner.run(
						config,
						runTimeProperties);
			}
			else {
				return singleSamplekmeansJobRunner.run(
						config,
						runTimeProperties);
			}
		}

		public void setZoomLevel(
				final int zoomLevel ) {
			parallelJobRunner.setZoomLevel(zoomLevel);
			singleSamplekmeansJobRunner.setZoomLevel(zoomLevel);
		}

		public void setInputFormatConfiguration(
				final FormatConfiguration inputFormatConfiguration ) {
			parallelJobRunner.setInputFormatConfiguration(inputFormatConfiguration);
			singleSamplekmeansJobRunner.setInputFormatConfiguration(inputFormatConfiguration);
		}

	}
}
