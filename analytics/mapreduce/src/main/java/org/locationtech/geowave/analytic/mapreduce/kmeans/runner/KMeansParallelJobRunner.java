/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.kmeans.runner;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.analytic.distance.FeatureCentroidDistanceFn;
import org.locationtech.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import org.locationtech.geowave.analytic.extract.SimpleFeatureGeometryExtractor;
import org.locationtech.geowave.analytic.mapreduce.MapReduceJobController;
import org.locationtech.geowave.analytic.mapreduce.MapReduceJobRunner;
import org.locationtech.geowave.analytic.mapreduce.clustering.runner.ClusteringRunner;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.FormatConfiguration;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.SampleParameters;
import org.opengis.feature.simple.SimpleFeature;

/**
 * The KMeans Parallel algorithm,labeled Algorithm 2 within in section 3.3 of
 *
 * <p>Bahmani, Kumar, Moseley, Vassilvitskii and Vattani. Scalable K-means++. VLDB Endowment Vol. 5,
 * No. 7. 2012.
 *
 * <!-- @formatter:off --> Couple things to note:
 *     <p>(1) Updating the cost of each sampled point occurs as the first step within sampling loop;
 *     the initial sample is performed outside the loop.
 *     <p>(2) A final update cost occurs outside the sampling loop just prior to stripping off the
 *     top 'K' centers.
 * <!-- @formatter:on -->
 */
public class KMeansParallelJobRunner extends MapReduceJobController implements ClusteringRunner {
  final SampleMultipleSetsJobRunner<SimpleFeature> sampleSetsRunner =
      new SampleMultipleSetsJobRunner<>();
  final StripWeakCentroidsRunner<SimpleFeature> stripWeakCentroidsRunner =
      new StripWeakCentroidsRunner<>();
  final KMeansIterationsJobRunner<SimpleFeature> kmeansJobRunner =
      new KMeansIterationsJobRunner<>();

  private int currentZoomLevel = 1;

  public KMeansParallelJobRunner() {
    // defaults
    setZoomLevel(1);

    // sts of child runners
    init(
        new MapReduceJobRunner[] {
            sampleSetsRunner,
            stripWeakCentroidsRunner, // run this one more
            // time with
            // 'smaller' size
            kmeansJobRunner},
        new PostOperationTask[] {DoNothingTask, DoNothingTask, new PostOperationTask() {

          @Override
          public void runTask(final Configuration config, final MapReduceJobRunner runner) {
            kmeansJobRunner.setReducerCount(stripWeakCentroidsRunner.getCurrentCentroidCount());
          }
        }, DoNothingTask});
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

    stripWeakCentroidsRunner.setRange(
        propertyManagement.getPropertyAsInt(SampleParameters.Sample.MIN_SAMPLE_SIZE, 2),
        propertyManagement.getPropertyAsInt(SampleParameters.Sample.MAX_SAMPLE_SIZE, 1000));

    ClusteringUtils.createAdapter(propertyManagement);
    ClusteringUtils.createIndex(propertyManagement);

    return super.run(config, propertyManagement);
  }

  @Override
  public Collection<ParameterEnum<?>> getParameters() {
    final Set<ParameterEnum<?>> params = new HashSet<>();
    params.addAll(kmeansJobRunner.getParameters());
    params.addAll(sampleSetsRunner.getParameters());
    // while override
    params.remove(CentroidParameters.Centroid.ZOOM_LEVEL);
    return params;
  }
}
