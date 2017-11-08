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
package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.geotools.feature.type.BasicFeatureTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.IndependentJobRunner;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.runner.GeoWaveInputLoadJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce.PassthruPartitioner;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters.Clustering;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters.Partition;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner;

/**
 * DBScan involves multiple iterations. The first iteration conceivably takes a
 * set of points and produces small clusters (nearest neighbors). Each
 * subsequent iteration merges clusters within a given distance from each other.
 * This process can continue no new clusters are created (merges do not occur).
 * 
 * The first iteration places a constraint on the minimum number of neighbors.
 * Subsequent iterations do not have a minimum, since each of the clusters is
 * already vetted out by the first iteration.
 */

public class DBScanIterationsJobRunner implements
		MapReduceJobRunner,
		IndependentJobRunner
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(DBScanIterationsJobRunner.class);
	DBScanJobRunner jobRunner = new DBScanJobRunner();
	GeoWaveInputLoadJobRunner inputLoadRunner = new GeoWaveInputLoadJobRunner();
	protected FormatConfiguration inputFormatConfiguration;
	protected int zoomLevel = 1;

	public DBScanIterationsJobRunner() {
		super();
		inputFormatConfiguration = new GeoWaveInputFormatConfiguration();
		jobRunner.setInputFormatConfiguration(inputFormatConfiguration);
		inputLoadRunner.setOutputFormatConfiguration(new GeoWaveOutputFormatConfiguration());
	}

	public void setInputFormatConfiguration(
			final FormatConfiguration inputFormatConfiguration ) {
		this.inputFormatConfiguration = inputFormatConfiguration;
	}

	public void setReducerCount(
			final int reducerCount ) {
		jobRunner.setReducerCount(reducerCount);
	}

	protected void setZoomLevel(
			final int zoomLevel ) {
		this.zoomLevel = zoomLevel;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		runTimeProperties.storeIfEmpty(
				GlobalParameters.Global.BATCH_ID,
				UUID.randomUUID().toString());

		FileSystem fs = null;
		try {
			fs = FileSystem.get(config);
			final String outputBaseDir = runTimeProperties.getPropertyAsString(
					MapReduceParameters.MRConfig.HDFS_BASE_DIR,
					"/tmp");

			Path startPath = new Path(
					outputBaseDir + "/level_0");
			if (fs.exists(startPath)) {
				// HPFortify "Path Manipulation"
				// False positive - path is internally managed
				fs.delete(
						startPath,
						true);
			}

			runTimeProperties.storeIfEmpty(
					Partition.PARTITIONER_CLASS,
					OrthodromicDistancePartitioner.class);

			final double maxDistance = runTimeProperties.getPropertyAsDouble(
					Partition.MAX_DISTANCE,
					10);

			final double precisionDecreaseRate = runTimeProperties.getPropertyAsDouble(
					Partition.PARTITION_DECREASE_RATE,
					0.15);

			double precisionFactor = runTimeProperties.getPropertyAsDouble(
					Partition.PARTITION_PRECISION,
					1.0);

			runTimeProperties.storeIfEmpty(
					Partition.DISTANCE_THRESHOLDS,
					Double.toString(maxDistance));

			final boolean overrideSecondary = runTimeProperties.hasProperty(Partition.SECONDARY_PARTITIONER_CLASS);

			if (!overrideSecondary) {
				final Serializable distances = runTimeProperties.get(Partition.DISTANCE_THRESHOLDS);
				String dstStr;
				if (distances == null) {
					dstStr = "0.000001";
				}
				else {
					dstStr = distances.toString();
				}
				final String distancesArray[] = dstStr.split(",");
				final double[] distancePerDimension = new double[distancesArray.length];
				{
					int i = 0;
					for (final String eachDistance : distancesArray) {
						distancePerDimension[i++] = Double.valueOf(eachDistance);
					}
				}
				boolean secondary = precisionFactor < 1.0;
				double total = 1.0;
				for (final double dist : distancePerDimension) {
					total *= dist;
				}
				secondary |= (total >= (Math.pow(
						maxDistance,
						distancePerDimension.length) * 2.0));
				if (secondary) {
					runTimeProperties.copy(
							Partition.PARTITIONER_CLASS,
							Partition.SECONDARY_PARTITIONER_CLASS);
				}
			}

			jobRunner.setInputFormatConfiguration(inputFormatConfiguration);
			jobRunner.setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration(
					startPath));

			LOGGER.info(
					"Running with partition distance {}",
					maxDistance);
			// HP Fortify "Command Injection" false positive
			// What Fortify considers "externally-influenced input"
			// comes only from users with OS-level access anyway
			final int initialStatus = jobRunner.run(
					config,
					runTimeProperties);

			if (initialStatus != 0) {
				return initialStatus;
			}

			precisionFactor = precisionFactor - precisionDecreaseRate;

			int maxIterationCount = runTimeProperties.getPropertyAsInt(
					ClusteringParameters.Clustering.MAX_ITERATIONS,
					15);

			int iteration = 2;
			long lastRecordCount = 0;

			while ((maxIterationCount > 0) && (precisionFactor > 0)) {

				// context does not mater in this case

				try {
					final Partitioner<?> partitioner = runTimeProperties.getClassInstance(
							PartitionParameters.Partition.PARTITIONER_CLASS,
							Partitioner.class,
							OrthodromicDistancePartitioner.class);

					partitioner.initialize(
							Job.getInstance(config),
							partitioner.getClass());
				}
				catch (final IllegalArgumentException argEx) {
					// this occurs if the partitioner decides that the distance
					// is
					// invalid (e.g. bigger than the map space).
					// In this case, we just exist out of the loop.
					// startPath has the final data
					LOGGER.info(
							"Distance is invalid",
							argEx);
					break;
				}
				catch (final Exception e1) {
					throw new IOException(
							e1);
				}

				final PropertyManagement localScopeProperties = new PropertyManagement(
						runTimeProperties);

				/**
				 * Re-partitioning the fat geometries can force a large number
				 * of partitions. The geometries end up being represented in
				 * multiple partitions. Better to skip secondary partitioning.
				 * 0.9 is a bit of a magic number. Ideally, it is based on the
				 * area of the max distance cube divided by the area as defined
				 * by threshold distances. However, looking up the partition
				 * dimension space or assuming only two dimensions were both
				 * undesirable.
				 */
				if ((precisionFactor <= 0.9) && !overrideSecondary) {
					localScopeProperties.store(
							Partition.SECONDARY_PARTITIONER_CLASS,
							PassthruPartitioner.class);
				}

				localScopeProperties.store(
						Partition.PARTITION_PRECISION,
						precisionFactor);
				jobRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
						startPath));

				jobRunner.setFirstIteration(false);

				localScopeProperties.store(
						HullParameters.Hull.ZOOM_LEVEL,
						zoomLevel);

				localScopeProperties.store(
						HullParameters.Hull.ITERATION,
						iteration);

				localScopeProperties.storeIfEmpty(
						OutputParameters.Output.DATA_TYPE_ID,
						localScopeProperties.getPropertyAsString(
								HullParameters.Hull.DATA_TYPE_ID,
								"concave_hull"));

				// Set to zero to force each cluster to be moved into the next
				// iteration
				// even if no merge occurs
				localScopeProperties.store(
						ClusteringParameters.Clustering.MINIMUM_SIZE,
						0);

				final Path nextPath = new Path(
						outputBaseDir + "/level_" + iteration);

				if (fs.exists(nextPath)) {
					// HPFortify "Path Manipulation"
					// False positive - path is internally managed
					fs.delete(
							nextPath,
							true);
				}
				jobRunner.setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration(
						nextPath));

				// HP Fortify "Command Injection" false positive
				// What Fortify considers "externally-influenced input"
				// comes only from users with OS-level access anyway
				final int status = jobRunner.run(
						config,
						localScopeProperties);

				if (status != 0) {
					return status;
				}

				final long currentOutputCount = jobRunner.getCounterValue(TaskCounter.REDUCE_OUTPUT_RECORDS);
				if (currentOutputCount == lastRecordCount) {
					maxIterationCount = 0;
				}
				lastRecordCount = currentOutputCount;
				startPath = nextPath;
				maxIterationCount--;
				precisionFactor -= precisionDecreaseRate;
				iteration++;
			}
			final PropertyManagement localScopeProperties = new PropertyManagement(
					runTimeProperties);

			localScopeProperties.storeIfEmpty(
					OutputParameters.Output.DATA_TYPE_ID,
					localScopeProperties.getPropertyAsString(
							HullParameters.Hull.DATA_TYPE_ID,
							"concave_hull"));
			localScopeProperties.storeIfEmpty(
					OutputParameters.Output.DATA_NAMESPACE_URI,
					localScopeProperties.getPropertyAsString(
							HullParameters.Hull.DATA_NAMESPACE_URI,
							BasicFeatureTypes.DEFAULT_NAMESPACE));
			localScopeProperties.storeIfEmpty(
					OutputParameters.Output.INDEX_ID,
					localScopeProperties.get(HullParameters.Hull.INDEX_ID));
			inputLoadRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
					startPath));
			// HP Fortify "Command Injection" false positive
			// What Fortify considers "externally-influenced input"
			// comes only from users with OS-level access anyway
			inputLoadRunner.run(
					config,
					runTimeProperties);
		}
		finally {
			if (fs != null) fs.close();
		}
		return 0;
	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
		params.addAll(jobRunner.getParameters());
		params.addAll(inputLoadRunner.getParameters());
		params.add(Clustering.MAX_ITERATIONS);
		params.add(Partition.PARTITION_DECREASE_RATE);
		params.add(Partition.PARTITION_PRECISION);
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
