/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
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
import org.apache.hadoop.conf.Configuration;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.CentroidManagerGeoWave;
import org.locationtech.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import org.locationtech.geowave.analytic.distance.FeatureCentroidDistanceFn;
import org.locationtech.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import org.locationtech.geowave.analytic.extract.SimpleFeatureGeometryExtractor;
import org.locationtech.geowave.analytic.mapreduce.MapReduceJobController;
import org.locationtech.geowave.analytic.mapreduce.MapReduceJobRunner;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.ClusteringParameters;
import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.FormatConfiguration;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.analytic.param.MapReduceParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.SampleParameters;
import org.locationtech.geowave.analytic.param.StoreParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Loop and sample multiple sets of K centers.
 *
 * Fulfills steps 3 through 5 in the Kmeans Parellel initialize Algorithm 2,as documented in section
 * 3.3 in
 *
 * Bahmani, Kumar, Moseley, Vassilvitskii and Vattani. Scalable K-means++. VLDB Endowment Vol. 5,
 * No. 7. 2012.
 *
 * The number of iterations is assumed to be log(psi), according the paper.
 *
 * As an added bonus, remove those centers that did not have sufficient number of matches, leaving
 * the top sampleSize/iterations.
 *
 */
public class SampleMultipleSetsJobRunner<T> extends MapReduceJobController implements
    MapReduceJobRunner {

  protected static final Logger LOGGER = LoggerFactory.getLogger(SampleMultipleSetsJobRunner.class);

  private final KSamplerJobRunner initialSampleRunner = new KSamplerJobRunner();
  private final UpdateCentroidCostJobRunner updateCostRunner = new UpdateCentroidCostJobRunner();
  private final RankSamplerJobRunner jobGrowSampleRunner = new RankSamplerJobRunner();
  private final StripWeakCentroidsRunner<T> stripWeakCentroidsRunner =
      new StripWeakCentroidsRunner<>();
  private final IterationCountCalculateRunner<T> iterationCountCalculateRunner =
      new IterationCountCalculateRunner<>();
  private int iterations = 1;
  private int zoomLevel = 1;

  public SampleMultipleSetsJobRunner() {
    stage1Setup();
  }

  private void stage1Setup() {
    init(
        new MapReduceJobRunner[] {
            initialSampleRunner,
            updateCostRunner,
            iterationCountCalculateRunner},
        new PostOperationTask[] {DoNothingTask, DoNothingTask, DoNothingTask});
  }

  public int getCurrentCentroidCount() {
    return stripWeakCentroidsRunner.getCurrentCentroidCount();
  }

  private void stage2Setup(final PropertyManagement runTimeProperties) {
    setIterations(iterationCountCalculateRunner.getIterationsCount());
    init(
        new MapReduceJobRunner[] {jobGrowSampleRunner, updateCostRunner, stripWeakCentroidsRunner},
        new PostOperationTask[] {DoNothingTask, DoNothingTask, new PostOperationTask() {
          @Override
          public void runTask(final Configuration config, final MapReduceJobRunner runner) {
            updateCostRunner.setReducerCount(
                Math.min(
                    stripWeakCentroidsRunner.getCurrentCentroidCount(),
                    runTimeProperties.getPropertyAsInt(
                        ClusteringParameters.Clustering.MAX_REDUCER_COUNT,
                        32)));
          }
        }});
  }

  @Override
  public int run(final Configuration config, final PropertyManagement runTimeProperties)
      throws Exception {

    // run stage 1
    updateCostRunner.setReducerCount(1);

    this.stripWeakCentroidsRunner.setRange(
        runTimeProperties.getPropertyAsInt(SampleParameters.Sample.MIN_SAMPLE_SIZE, 2),
        runTimeProperties.getPropertyAsInt(SampleParameters.Sample.MAX_SAMPLE_SIZE, 1000));

    runTimeProperties.store(
        SampleParameters.Sample.SAMPLE_SIZE,
        runTimeProperties.getPropertyAsInt(SampleParameters.Sample.MAX_SAMPLE_SIZE, 1000));

    setIterations(runTimeProperties.getPropertyAsInt(SampleParameters.Sample.SAMPLE_ITERATIONS, 1));

    runTimeProperties.storeIfEmpty(
        CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
        SimpleFeatureItemWrapperFactory.class);

    runTimeProperties.storeIfEmpty(
        CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
        FeatureCentroidDistanceFn.class);

    runTimeProperties.storeIfEmpty(
        CentroidParameters.Centroid.EXTRACTOR_CLASS,
        SimpleFeatureCentroidExtractor.class);

    runTimeProperties.storeIfEmpty(
        CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
        SimpleFeatureGeometryExtractor.class);

    runTimeProperties.copy(
        CentroidParameters.Centroid.DATA_TYPE_ID,
        SampleParameters.Sample.DATA_TYPE_NAME);

    runTimeProperties.copy(
        CentroidParameters.Centroid.INDEX_NAME,
        SampleParameters.Sample.INDEX_NAME);

    runTimeProperties.store(CentroidParameters.Centroid.ZOOM_LEVEL, zoomLevel);

    stage1Setup();
    final int status1 = super.run(config, runTimeProperties);
    if (status1 != 0) {
      return status1;
    }
    stage2Setup(runTimeProperties);

    for (int i = 0; i < iterations; i++) {
      final int status2 = super.run(config, runTimeProperties);
      if (status2 != 0) {
        return status2;
      }
    }
    return 0;
  }

  @Override
  public Collection<ParameterEnum<?>> getParameters() {
    final Set<ParameterEnum<?>> params = new HashSet<>();
    params.addAll(
        Arrays.asList(
            new ParameterEnum<?>[] {
                SampleParameters.Sample.MAX_SAMPLE_SIZE,
                SampleParameters.Sample.SAMPLE_ITERATIONS,
                SampleParameters.Sample.MIN_SAMPLE_SIZE,
                CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
                CentroidParameters.Centroid.INDEX_NAME,
                CentroidParameters.Centroid.DATA_TYPE_ID,
                CentroidParameters.Centroid.DATA_NAMESPACE_URI,
                CentroidParameters.Centroid.EXTRACTOR_CLASS,
                CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
                CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
                StoreParameters.StoreParam.INPUT_STORE,
                GlobalParameters.Global.BATCH_ID}));

    params.addAll(MapReduceParameters.getParameters());

    params.addAll(NestedGroupCentroidAssignment.getParameters());
    params.addAll(CentroidManagerGeoWave.getParameters());
    params.addAll(initialSampleRunner.getParameters());
    return params;
  }

  public void setInputFormatConfiguration(final FormatConfiguration inputFormatConfiguration) {
    initialSampleRunner.setInputFormatConfiguration(inputFormatConfiguration);
    updateCostRunner.setInputFormatConfiguration(inputFormatConfiguration);
    jobGrowSampleRunner.setInputFormatConfiguration(inputFormatConfiguration);
  }

  private void setIterations(final int iterations) {
    this.iterations = Math.max(this.iterations, iterations);
  }

  public void setZoomLevel(final int zoomLevel) {
    this.zoomLevel = zoomLevel;
    initialSampleRunner.setZoomLevel(zoomLevel);
    jobGrowSampleRunner.setZoomLevel(zoomLevel);
  }
}
