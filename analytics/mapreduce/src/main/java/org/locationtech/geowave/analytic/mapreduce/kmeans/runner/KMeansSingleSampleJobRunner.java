/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.kmeans.runner;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.analytic.clustering.NestedGroupCentroidAssignment;
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
import org.locationtech.geowave.analytic.param.MapReduceParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.SampleParameters;
import org.locationtech.geowave.analytic.param.StoreParameters;

/** */
public class KMeansSingleSampleJobRunner<T> extends MapReduceJobController implements
    ClusteringRunner {
  final KSamplerJobRunner sampleSetsRunner = new KSamplerJobRunner();
  final KMeansIterationsJobRunner<T> kmeansJobRunner = new KMeansIterationsJobRunner<>();

  private int currentZoomLevel = 1;

  public KMeansSingleSampleJobRunner() {
    // defaults
    setZoomLevel(1);

    // sets of child runners
    init(
        new MapReduceJobRunner[] {sampleSetsRunner, kmeansJobRunner},
        new PostOperationTask[] {DoNothingTask, DoNothingTask});
  }

  @Override
  public void setZoomLevel(final int zoomLevel) {
    currentZoomLevel = zoomLevel;
    sampleSetsRunner.setZoomLevel(zoomLevel);
  }

  @Override
  public void setInputFormatConfiguration(final FormatConfiguration inputFormatConfiguration) {
    sampleSetsRunner.setInputFormatConfiguration(inputFormatConfiguration);
    kmeansJobRunner.setInputFormatConfiguration(inputFormatConfiguration);
  }

  @Override
  public int run(final Configuration configuration, final PropertyManagement propertyManagement)
      throws Exception {
    return runJob(configuration, propertyManagement);
  }

  private int runJob(final Configuration config, final PropertyManagement propertyManagement)
      throws Exception {

    propertyManagement.store(CentroidParameters.Centroid.ZOOM_LEVEL, currentZoomLevel);

    propertyManagement.storeIfEmpty(GlobalParameters.Global.BATCH_ID, UUID.randomUUID().toString());

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

    ClusteringUtils.createAdapter(propertyManagement);
    ClusteringUtils.createIndex(propertyManagement);

    return super.run(config, propertyManagement);
  }

  @Override
  public Collection<ParameterEnum<?>> getParameters() {
    final Set<ParameterEnum<?>> params = new HashSet<>();
    params.addAll(kmeansJobRunner.getParameters());
    params.addAll(
        Arrays.asList(
            new ParameterEnum<?>[] {
                ClusteringParameters.Clustering.MAX_REDUCER_COUNT,
                SampleParameters.Sample.SAMPLE_SIZE,
                SampleParameters.Sample.SAMPLE_RANK_FUNCTION,
                CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
                CentroidParameters.Centroid.INDEX_NAME,
                CentroidParameters.Centroid.DATA_TYPE_ID,
                CentroidParameters.Centroid.DATA_NAMESPACE_URI,
                CentroidParameters.Centroid.EXTRACTOR_CLASS,
                CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
                CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
                StoreParameters.StoreParam.INPUT_STORE,
                GlobalParameters.Global.BATCH_ID,
                ClusteringParameters.Clustering.MAX_REDUCER_COUNT}));
    params.addAll(MapReduceParameters.getParameters());
    params.addAll(NestedGroupCentroidAssignment.getParameters());

    // override
    params.remove(CentroidParameters.Centroid.ZOOM_LEVEL);
    params.remove(SampleParameters.Sample.DATA_TYPE_NAME);
    params.remove(SampleParameters.Sample.INDEX_NAME);
    return params;
  }
}
