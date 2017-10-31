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
package mil.nga.giat.geowave.analytic.mapreduce.kmeans.runner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.IndependentJobRunner;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager.CentroidProcessingFn;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Run 'K' means until convergence across ALL groups.
 * 
 * 
 */
public class KMeansIterationsJobRunner<T> implements
		MapReduceJobRunner,
		IndependentJobRunner
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(KMeansIterationsJobRunner.class);

	private final KMeansJobRunner jobRunner = new KMeansJobRunner();
	private double convergenceTol = 0.0001;

	public KMeansIterationsJobRunner() {}

	protected CentroidManager<T> constructCentroidManager(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws IOException {
		return new CentroidManagerGeoWave<T>(
				runTimeProperties);
	}

	public void setInputFormatConfiguration(
			final FormatConfiguration inputFormatConfiguration ) {
		jobRunner.setInputFormatConfiguration(inputFormatConfiguration);
	}

	public void setReducerCount(
			final int reducerCount ) {
		jobRunner.setReducerCount(reducerCount);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		convergenceTol = runTimeProperties.getPropertyAsDouble(
				ClusteringParameters.Clustering.CONVERGANCE_TOLERANCE,
				convergenceTol);

		final DistanceFn<T> distanceFunction = runTimeProperties.getClassInstance(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				DistanceFn.class,
				FeatureCentroidDistanceFn.class);

		int maxIterationCount = runTimeProperties.getPropertyAsInt(
				ClusteringParameters.Clustering.MAX_ITERATIONS,
				15);
		boolean converged = false;

		while (!converged && (maxIterationCount > 0)) {
			final int status = runJob(
					config,
					runTimeProperties);
			if (status != 0) {
				return status;
			}

			// new one each time to force a refresh of the centroids
			final CentroidManager<T> centroidManager = constructCentroidManager(
					config,
					runTimeProperties);

			// check for convergence
			converged = checkForConvergence(
					centroidManager,
					distanceFunction);

			maxIterationCount--;
		}
		return 0;
	}

	protected int runJob(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		runTimeProperties.storeIfEmpty(
				CentroidParameters.Centroid.EXTRACTOR_CLASS,
				SimpleFeatureCentroidExtractor.class);
		runTimeProperties.storeIfEmpty(
				CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);
		runTimeProperties.storeIfEmpty(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);
		// HP Fortify "Command Injection" false positive
		// What Fortify considers "externally-influenced input"
		// comes only from users with OS-level access anyway
		return jobRunner.run(
				config,
				runTimeProperties);
	}

	private boolean checkForConvergence(
			final CentroidManager<T> centroidManager,
			final DistanceFn<T> distanceFunction )
			throws IOException {
		final AtomicInteger grpCount = new AtomicInteger(
				0);
		final AtomicInteger failuresCount = new AtomicInteger(
				0);
		final AtomicInteger centroidCount = new AtomicInteger(
				0);
		final boolean status = centroidManager.processForAllGroups(new CentroidProcessingFn<T>() {
			@Override
			public int processGroup(
					final String groupID,
					final List<AnalyticItemWrapper<T>> centroids ) {
				grpCount.incrementAndGet();
				centroidCount.addAndGet(centroids.size() / 2);

				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace(
							"Parent Group: {} ",
							groupID);
					for (final AnalyticItemWrapper<T> troid : centroids) {
						LOGGER.warn(
								"Child Group: {} ",
								troid.getID());
					}
				}
				failuresCount.addAndGet(computeCostAndCleanUp(
						groupID,
						centroids,
						centroidManager,
						distanceFunction));
				return 0;
			}

		}) == 0 ? true : false;
		// update default based on data size
		setReducerCount(grpCount.get() * centroidCount.get());
		return status && (failuresCount.get() == 0);
	}

	protected int computeCostAndCleanUp(
			final String groupID,
			final List<AnalyticItemWrapper<T>> centroids,
			final CentroidManager<T> centroidManager,
			final DistanceFn<T> distanceFunction ) {
		double distance = 0;
		final List<String> deletionKeys = new ArrayList<String>();

		// sort by id and then by iteration
		Collections.sort(
				centroids,
				new Comparator<AnalyticItemWrapper<T>>() {

					@Override
					public int compare(
							final AnalyticItemWrapper<T> arg0,
							final AnalyticItemWrapper<T> arg1 ) {
						final int c = arg0.getName().compareTo(
								arg1.getName());
						if (c == 0) {
							return arg0.getIterationID() - arg1.getIterationID();
						}
						else {
							return c;
						}
					}
				});
		AnalyticItemWrapper<T> prior = null;
		for (final AnalyticItemWrapper<T> centroid : centroids) {
			if (prior == null) {
				prior = centroid;
				continue;
			}
			else if (!prior.getName().equals(
					centroid.getName())) {
				// should we delete this...it is a centroid without assigned
				// points? This occurs when the number of centroids exceeds the
				// number of points in a cluster.
				// it is an edge case.
				// deletionKeys.add( prior.getID() );
				LOGGER.warn("Centroid is no longer viable " + prior.getID() + " from group " + prior.getGroupID());
				prior = centroid;
				continue;
			}
			// the prior run centroids are still present from the geowave data
			// store;
			// their priors do not exist in the map
			distance += distanceFunction.measure(
					prior.getWrappedItem(),
					centroid.getWrappedItem());
			deletionKeys.add(prior.getID());
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace(
						"Within group {} replace {} with {}",
						new String[] {
							prior.getGroupID(),
							prior.getID(),
							centroid.getID()
						});
			}
			prior = null;
		}
		distance /= centroids.size();

		try {
			centroidManager.delete(deletionKeys.toArray(new String[deletionKeys.size()]));
		}
		catch (final IOException e) {
			throw new RuntimeException(
					e);
		}

		return (distance < convergenceTol) ? 0 : 1;
	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			CentroidParameters.Centroid.INDEX_ID,
			CentroidParameters.Centroid.DATA_TYPE_ID,
			CentroidParameters.Centroid.DATA_NAMESPACE_URI,
			CentroidParameters.Centroid.EXTRACTOR_CLASS,
			CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
			ClusteringParameters.Clustering.MAX_REDUCER_COUNT,
			ClusteringParameters.Clustering.MAX_ITERATIONS,
			ClusteringParameters.Clustering.CONVERGANCE_TOLERANCE,
			CommonParameters.Common.DISTANCE_FUNCTION_CLASS
		}));

		params.addAll(CentroidManagerGeoWave.getParameters());
		params.addAll(NestedGroupCentroidAssignment.getParameters());
		params.addAll(jobRunner.getParameters());
		return params;
	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				MapReduceJobController.getConfiguration(runTimeProperties),
				runTimeProperties);
	}
}
