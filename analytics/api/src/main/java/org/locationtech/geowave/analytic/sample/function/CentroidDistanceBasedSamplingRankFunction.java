/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.sample.function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.analytic.AnalyticItemWrapper;
import org.locationtech.geowave.analytic.AnalyticItemWrapperFactory;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.ScopedJobConfiguration;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.CentroidPairing;
import org.locationtech.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import org.locationtech.geowave.analytic.distance.DistanceFn;
import org.locationtech.geowave.analytic.kmeans.AssociationNotification;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.SampleParameters;
import org.locationtech.geowave.analytic.sample.RandomProbabilitySampleFn;
import org.locationtech.geowave.analytic.sample.SampleProbabilityFn;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rank objects using their distance to the closest centroid of a set of centroids. The specific
 * rank is determined by the probability of the point meeting being a centroid, modeled in the
 * implementation of {@link SampleProbabilityFn}.
 *
 * <p>The farther the distance, the higher the rank.
 *
 * <!-- @formatter:off --> Properties:
 *     <p>"CentroidDistanceBasedSamplingRankFunction.KMeansConfig.data_store_configuration" - The
 *     class used to determine the prefix class name for te GeoWave Data Store parameters for a
 *     connection to collect the starting set of centroids. Defaults to {@link
 *     CentroidDistanceBasedSamplingRankFunction}.
 *     <p>"CentroidDistanceBasedSamplingRankFunction.KMeansConfig.probability_function" -
 *     implementation of {@link SampleProbabilityFn}
 *     <p>"CentroidDistanceBasedSamplingRankFunction.KMeansConfig.distance_function" - {@link
 *     DistanceFn}
 *     <p>"CentroidDistanceBasedSamplingRankFunction.KMeansConfig.centroid_factory" - {@link
 *     AnalyticItemWrapperFactory} to wrap the centroid data with the appropriate centroid wrapper
 *     {@link AnalyticItemWrapper}
 * <!-- @formatter:on -->
 *     <p>See {@link GeoWaveConfiguratorBase} for information for configuration GeoWave Data Store
 *     for consumption of starting set of centroids.
 * @param <T> The data type for the object being sampled
 */
public class CentroidDistanceBasedSamplingRankFunction<T> implements SamplingRankFunction<T> {

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(CentroidDistanceBasedSamplingRankFunction.class);

  private SampleProbabilityFn sampleProbabilityFn;
  private NestedGroupCentroidAssignment<T> nestedGroupCentroidAssigner;
  private final Map<String, Double> groupToConstant = new HashMap<>();
  protected AnalyticItemWrapperFactory<T> itemWrapperFactory;;

  public static void setParameters(
      final Configuration config,
      final Class<?> scope,
      final PropertyManagement runTimeProperties) {
    NestedGroupCentroidAssignment.setParameters(config, scope, runTimeProperties);
    runTimeProperties.setConfig(
        new ParameterEnum[] {
            SampleParameters.Sample.PROBABILITY_FUNCTION,
            CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,},
        config,
        scope);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initialize(final JobContext context, final Class<?> scope, final Logger logger)
      throws IOException {
    final ScopedJobConfiguration config =
        new ScopedJobConfiguration(context.getConfiguration(), scope);
    try {
      sampleProbabilityFn =
          config.getInstance(
              SampleParameters.Sample.PROBABILITY_FUNCTION,
              SampleProbabilityFn.class,
              RandomProbabilitySampleFn.class);
    } catch (final Exception e) {
      throw new IOException(e);
    }

    try {
      itemWrapperFactory =
          config.getInstance(
              CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
              AnalyticItemWrapperFactory.class,
              SimpleFeatureItemWrapperFactory.class);

      itemWrapperFactory.initialize(context, scope, logger);
    } catch (final Exception e1) {

      throw new IOException(e1);
    }

    try {
      nestedGroupCentroidAssigner = new NestedGroupCentroidAssignment<>(context, scope, logger);
    } catch (final Exception e1) {
      throw new IOException(e1);
    }
  }

  /** */
  @Override
  public double rank(final int sampleSize, final T value) {
    final AnalyticItemWrapper<T> item = itemWrapperFactory.create(value);
    final List<AnalyticItemWrapper<T>> centroids = new ArrayList<>();
    double weight;
    try {
      weight =
          nestedGroupCentroidAssigner.findCentroidForLevel(item, new AssociationNotification<T>() {
            @Override
            public void notify(final CentroidPairing<T> pairing) {
              try {
                centroids.addAll(
                    nestedGroupCentroidAssigner.getCentroidsForGroup(
                        pairing.getCentroid().getGroupID()));
              } catch (final IOException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return sampleProbabilityFn.getProbability(
        weight,
        getNormalizingConstant(centroids.get(0).getGroupID(), centroids),
        sampleSize);
  }

  private double getNormalizingConstant(
      final String groupID,
      final List<AnalyticItemWrapper<T>> centroids) {

    if (!groupToConstant.containsKey(groupID)) {
      double constant = 0.0;
      for (final AnalyticItemWrapper<T> centroid : centroids) {
        constant += centroid.getCost();
      }
      groupToConstant.put(groupID, constant);
    }
    return groupToConstant.get(groupID).doubleValue();
  }
}
