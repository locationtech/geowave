/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.clustering.runner;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.feature.type.BasicFeatureTypes;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import org.locationtech.geowave.analytic.extract.SimpleFeatureGeometryExtractor;
import org.locationtech.geowave.analytic.mapreduce.MapReduceJobController;
import org.locationtech.geowave.analytic.mapreduce.MapReduceJobRunner;
import org.locationtech.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import org.locationtech.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.ClusteringParameters.Clustering;
import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.ExtractParameters;
import org.locationtech.geowave.analytic.param.GlobalParameters.Global;
import org.locationtech.geowave.analytic.param.HullParameters;
import org.locationtech.geowave.analytic.param.MapReduceParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;

/**
 * Runs a clustering at multiple levels. Lower levels cluster within each cluster of the higher
 * level.
 *
 * <p> Steps:
 *
 * @formatter: off <p> (1) Extract and deduplicate items from geowave. <p> (2) Cluster item within
 *             their assigned groups. Initially, items are all part of the same group. <p> (3)
 *             Assign to each point the cluster (group id). <p> (4) Repeat steps 2 to 3 for each
 *             lower level.
 * @formatter: on
 */
public abstract class MultiLevelClusteringJobRunner extends MapReduceJobController
    implements MapReduceJobRunner {

  final GroupAssigmentJobRunner groupAssignmentRunner = new GroupAssigmentJobRunner();
  final GeoWaveAnalyticExtractJobRunner jobExtractRunner = new GeoWaveAnalyticExtractJobRunner();
  final ConvexHullJobRunner hullRunner = new ConvexHullJobRunner();

  public MultiLevelClusteringJobRunner() {
    init(new MapReduceJobRunner[] {}, new PostOperationTask[] {});
  }

  protected abstract ClusteringRunner getClusteringRunner();

  @Override
  public Collection<ParameterEnum<?>> getParameters() {
    final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
    params.addAll(jobExtractRunner.getParameters());
    params.addAll(hullRunner.getParameters());
    params.addAll(getClusteringRunner().getParameters());
    params.addAll(Arrays.asList(new ParameterEnum<?>[] {Clustering.ZOOM_LEVELS, Global.BATCH_ID}));
    params.addAll(MapReduceParameters.getParameters());
    // the output data type is used for centroid management
    params.remove(CentroidParameters.Centroid.DATA_TYPE_ID);

    params.remove(CentroidParameters.Centroid.DATA_NAMESPACE_URI);
    return params;
  }

  @Override
  public int run(final Configuration configuration, final PropertyManagement propertyManagement)
      throws Exception {
    return runJob(configuration, propertyManagement);
  }

  private int runJob(final Configuration config, final PropertyManagement propertyManagement)
      throws Exception {

    final ClusteringRunner clusteringRunner = getClusteringRunner();
    final Integer zoomLevels = propertyManagement.getPropertyAsInt(Clustering.ZOOM_LEVELS, 1);

    jobExtractRunner.setConf(config);

    final String dataTypeId =
        propertyManagement.getPropertyAsString(
            ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
            "centroid");

    final String namespaceURI =
        propertyManagement.getPropertyAsString(
            ExtractParameters.Extract.DATA_NAMESPACE_URI,
            BasicFeatureTypes.DEFAULT_NAMESPACE);

    propertyManagement.storeIfEmpty(ExtractParameters.Extract.DATA_NAMESPACE_URI, namespaceURI);

    propertyManagement.storeIfEmpty(ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID, dataTypeId);

    propertyManagement.storeIfEmpty(
        CentroidParameters.Centroid.EXTRACTOR_CLASS,
        SimpleFeatureCentroidExtractor.class);

    propertyManagement.storeIfEmpty(
        CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
        SimpleFeatureGeometryExtractor.class);

    propertyManagement.store(CentroidParameters.Centroid.DATA_TYPE_ID, dataTypeId);

    propertyManagement.store(CentroidParameters.Centroid.DATA_NAMESPACE_URI, namespaceURI);

    // TODO: set out index type for extracts?
    propertyManagement.storeIfEmpty(
        CentroidParameters.Centroid.INDEX_NAME,
        new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions()).getName());

    propertyManagement.storeIfEmpty(
        HullParameters.Hull.INDEX_NAME,
        new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions()).getName());

    // first. extract data
    int status = jobExtractRunner.run(config, propertyManagement);

    final Path extractPath = jobExtractRunner.getHdfsOutputPath();

    groupAssignmentRunner.setInputFormatConfiguration(
        new SequenceFileInputFormatConfiguration(extractPath));
    clusteringRunner.setInputFormatConfiguration(
        new SequenceFileInputFormatConfiguration(extractPath));
    hullRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(extractPath));

    final boolean retainGroupAssigments =
        propertyManagement.getPropertyAsBoolean(Clustering.RETAIN_GROUP_ASSIGNMENTS, false);

    // run clustering for each level
    final String outputBaseDir =
        propertyManagement.getPropertyAsString(MapReduceParameters.MRConfig.HDFS_BASE_DIR, "/tmp");
    FileSystem fs = null;
    try {
      fs = FileSystem.get(config);
      for (int i = 0; (status == 0) && (i < zoomLevels); i++) {
        final int zoomLevel = i + 1;
        clusteringRunner.setZoomLevel(zoomLevel);
        hullRunner.setZoomLevel(zoomLevel);
        // need to get this removed at some point.
        propertyManagement.store(CentroidParameters.Centroid.ZOOM_LEVEL, zoomLevel);
        status = clusteringRunner.run(config, propertyManagement);
        if (status == 0) {
          final Path nextPath = new Path(outputBaseDir + "/" + "level_" + zoomLevel);
          if (fs.exists(nextPath)) {
            // HPFortify "Path Manipulation"
            // False positive - path is internally managed
            fs.delete(nextPath, true);
          }

          groupAssignmentRunner.setOutputFormatConfiguration(
              new SequenceFileOutputFormatConfiguration(nextPath));
          groupAssignmentRunner.setZoomLevel(zoomLevel);

          // HP Fortify "Command Injection" false positive
          // What Fortify considers "externally-influenced input"
          // comes only from users with OS-level access anyway
          status =
              retainGroupAssigments ? groupAssignmentRunner.run(config, propertyManagement) : 0;

          if (status == 0) {
            // HP Fortify "Command Injection" false positive
            // What Fortify considers "externally-influenced input"
            // comes only from users with OS-level access anyway
            status = hullRunner.run(config, propertyManagement);
          }
          if (retainGroupAssigments) {
            clusteringRunner.setInputFormatConfiguration(
                new SequenceFileInputFormatConfiguration(nextPath));
            hullRunner.setInputFormatConfiguration(
                new SequenceFileInputFormatConfiguration(nextPath));
            groupAssignmentRunner.setInputFormatConfiguration(
                new SequenceFileInputFormatConfiguration(nextPath));
          }
        }
      }
      return status;
    } finally {
      if (fs != null)
        fs.close();
    }
  }
}
