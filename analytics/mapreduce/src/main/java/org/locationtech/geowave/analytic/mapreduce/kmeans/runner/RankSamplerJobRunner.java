/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.kmeans.runner;

import org.apache.hadoop.conf.Configuration;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.clustering.CentroidManagerGeoWave;
import org.locationtech.geowave.analytic.distance.DistanceFn;
import org.locationtech.geowave.analytic.mapreduce.MapReduceJobRunner;
import org.locationtech.geowave.analytic.param.SampleParameters;
import org.locationtech.geowave.analytic.sample.BahmanEtAlSampleProbabilityFn;
import org.locationtech.geowave.analytic.sample.function.CentroidDistanceBasedSamplingRankFunction;

/**
 * Sample K points given a sample function. The sampled K points are are stored as centroids within
 * GeoWave. The sampling weight may be determined by the relation of a point to a current set of
 * centroids, thus a {@link DistanceFn} instance is required.
 */
public class RankSamplerJobRunner extends KSamplerJobRunner implements MapReduceJobRunner {

  public RankSamplerJobRunner() {
    setSamplingRankFunctionClass(CentroidDistanceBasedSamplingRankFunction.class);
  }

  @Override
  public int run(final Configuration config, final PropertyManagement runTimeProperties)
      throws Exception {
    CentroidManagerGeoWave.setParameters(config, getScope(), runTimeProperties);
    runTimeProperties.storeIfEmpty(
        SampleParameters.Sample.PROBABILITY_FUNCTION,
        BahmanEtAlSampleProbabilityFn.class);
    CentroidDistanceBasedSamplingRankFunction.setParameters(config, getScope(), runTimeProperties);

    // HP Fortify "Command Injection" false positive
    // What Fortify considers "externally-influenced input"
    // comes only from users with OS-level access anyway
    return super.run(config, runTimeProperties);
  }
}
