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
package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureGeometryExtractor;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters.Clustering;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters.Global;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.feature.type.BasicFeatureTypes;

/**
 * Runs a clustering at multiple levels. Lower levels cluster within each
 * cluster of the higher level.
 * 
 * Steps:
 * 
 * @formatter: off
 * 
 *             (1) Extract and deduplicate items from geowave.
 * 
 *             (2) Cluster item within their assigned groups. Initially, items
 *             are all part of the same group.
 * 
 *             (3) Assign to each point the cluster (group id).
 * 
 *             (4) Repeat steps 2 to 3 for each lower level.
 * 
 * @formatter: on
 * 
 */
public abstract class MultiLevelClusteringJobRunner extends
		MapReduceJobController implements
		MapReduceJobRunner
{

	final GroupAssigmentJobRunner groupAssignmentRunner = new GroupAssigmentJobRunner();
	final GeoWaveAnalyticExtractJobRunner jobExtractRunner = new GeoWaveAnalyticExtractJobRunner();
	final ConvexHullJobRunner hullRunner = new ConvexHullJobRunner();

	public MultiLevelClusteringJobRunner() {
		init(
				new MapReduceJobRunner[] {},
				new PostOperationTask[] {});
	}

	protected abstract ClusteringRunner getClusteringRunner();

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
		params.addAll(jobExtractRunner.getParameters());
		params.addAll(hullRunner.getParameters());
		params.addAll(getClusteringRunner().getParameters());
		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			Clustering.ZOOM_LEVELS,
			Global.BATCH_ID
		}));
		params.addAll(MapReduceParameters.getParameters());
		// the output data type is used for centroid management
		params.remove(CentroidParameters.Centroid.DATA_TYPE_ID);

		params.remove(CentroidParameters.Centroid.DATA_NAMESPACE_URI);
		return params;
	}

	@Override
	public int run(
			final Configuration configuration,
			final PropertyManagement propertyManagement )
			throws Exception {
		return runJob(
				configuration,
				propertyManagement);
	}

	private int runJob(
			final Configuration config,
			final PropertyManagement propertyManagement )
			throws Exception {

		final ClusteringRunner clusteringRunner = getClusteringRunner();
		final Integer zoomLevels = propertyManagement.getPropertyAsInt(
				Clustering.ZOOM_LEVELS,
				1);

		jobExtractRunner.setConf(config);

		final String dataTypeId = propertyManagement.getPropertyAsString(
				ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
				"centroid");

		final String namespaceURI = propertyManagement.getPropertyAsString(
				ExtractParameters.Extract.DATA_NAMESPACE_URI,
				BasicFeatureTypes.DEFAULT_NAMESPACE);

		propertyManagement.storeIfEmpty(
				ExtractParameters.Extract.DATA_NAMESPACE_URI,
				namespaceURI);

		propertyManagement.storeIfEmpty(
				ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
				dataTypeId);

		propertyManagement.storeIfEmpty(
				CentroidParameters.Centroid.EXTRACTOR_CLASS,
				SimpleFeatureCentroidExtractor.class);

		propertyManagement.storeIfEmpty(
				CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
				SimpleFeatureGeometryExtractor.class);

		propertyManagement.store(
				CentroidParameters.Centroid.DATA_TYPE_ID,
				dataTypeId);

		propertyManagement.store(
				CentroidParameters.Centroid.DATA_NAMESPACE_URI,
				namespaceURI);

		// TODO: set out index type for extracts?
		propertyManagement.storeIfEmpty(
				CentroidParameters.Centroid.INDEX_ID,
				new SpatialDimensionalityTypeProvider().createPrimaryIndex(
						new SpatialOptions()).getId().getString());

		propertyManagement.storeIfEmpty(
				HullParameters.Hull.INDEX_ID,
				new SpatialDimensionalityTypeProvider().createPrimaryIndex(
						new SpatialOptions()).getId().getString());

		// first. extract data
		int status = jobExtractRunner.run(
				config,
				propertyManagement);

		final Path extractPath = jobExtractRunner.getHdfsOutputPath();

		groupAssignmentRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				extractPath));
		clusteringRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				extractPath));
		hullRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				extractPath));

		final boolean retainGroupAssigments = propertyManagement.getPropertyAsBoolean(
				Clustering.RETAIN_GROUP_ASSIGNMENTS,
				false);

		// run clustering for each level
		final String outputBaseDir = propertyManagement.getPropertyAsString(
				MapReduceParameters.MRConfig.HDFS_BASE_DIR,
				"/tmp");
		FileSystem fs = null;
		try {
			fs = FileSystem.get(config);
			for (int i = 0; (status == 0) && (i < zoomLevels); i++) {
				final int zoomLevel = i + 1;
				clusteringRunner.setZoomLevel(zoomLevel);
				hullRunner.setZoomLevel(zoomLevel);
				// need to get this removed at some point.
				propertyManagement.store(
						CentroidParameters.Centroid.ZOOM_LEVEL,
						zoomLevel);
				status = clusteringRunner.run(
						config,
						propertyManagement);
				if (status == 0) {
					final Path nextPath = new Path(
							outputBaseDir + "/" + "level_" + zoomLevel);
					if (fs.exists(nextPath)) {
						// HPFortify "Path Manipulation"
						// False positive - path is internally managed
						fs.delete(
								nextPath,
								true);
					}

					groupAssignmentRunner.setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration(
							nextPath));
					groupAssignmentRunner.setZoomLevel(zoomLevel);

					// HP Fortify "Command Injection" false positive
					// What Fortify considers "externally-influenced input"
					// comes only from users with OS-level access anyway
					status = retainGroupAssigments ? groupAssignmentRunner.run(
							config,
							propertyManagement) : 0;

					if (status == 0) {
						// HP Fortify "Command Injection" false positive
						// What Fortify considers "externally-influenced input"
						// comes only from users with OS-level access anyway
						status = hullRunner.run(
								config,
								propertyManagement);
					}
					if (retainGroupAssigments) {
						clusteringRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
								nextPath));
						hullRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
								nextPath));
						groupAssignmentRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
								nextPath));
					}
				}
			}
			return status;
		}
		finally {
			if (fs != null) fs.close();
		}
	}
}
