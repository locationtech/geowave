/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.clustering;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.locationtech.geowave.analytic.AnalyticItemWrapperFactory;
import org.locationtech.geowave.analytic.ScopedJobConfiguration;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.CentroidManagerGeoWave;
import org.locationtech.geowave.analytic.clustering.CentroidPairing;
import org.locationtech.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import org.locationtech.geowave.analytic.extract.CentroidExtractor;
import org.locationtech.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import org.locationtech.geowave.analytic.kmeans.AssociationNotification;
import org.locationtech.geowave.analytic.mapreduce.GroupIDText;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.mapreduce.GeoWaveWritableInputMapper;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adjust input items so that so that the assigned centroid becomes the group ID. If the item has an
 * assigned group ID, the resulting item's group ID is replaced in the output.
 *
 * <p>From a multi-level clustering algorithm, an item has a different grouping in each level. Items
 * are clustered within their respective groups.
 *
 * @formatter:off
 *     <p>Context configuration parameters include:
 *     <p>"GroupAssignmentMapReduce.Common.DistanceFunctionClass" -> Used to determine distance to
 *     centroid
 *     <p>"GroupAssignmentMapReduce.Centroid.ExtractorClass" -> {@link
 *     org.locationtech.geowave.analytic.extract.CentroidExtractor}
 *     <p>"GroupAssignmentMapReduce.Centroid.WrapperFactoryClass" -> {@link
 *     AnalyticItemWrapperFactory} to extract wrap spatial objects with Centroid management
 *     functions
 *     <p>"GroupAssignmentMapReduce.Centroid.ZoomLevel" -> The current zoom level
 * @see CentroidManagerGeoWave
 * @formatter:on
 */
public class GroupAssignmentMapReduce {

  protected static final Logger LOGGER = LoggerFactory.getLogger(GroupAssignmentMapReduce.class);

  public static class GroupAssignmentMapper extends
      GeoWaveWritableInputMapper<GeoWaveInputKey, ObjectWritable> {

    private NestedGroupCentroidAssignment<Object> nestedGroupCentroidAssigner;
    protected GroupIDText outputKeyWritable = new GroupIDText();
    protected ObjectWritable outputValWritable = new ObjectWritable();
    protected CentroidExtractor<Object> centroidExtractor;
    protected AnalyticItemWrapperFactory<Object> itemWrapperFactory;
    private final Map<String, AtomicInteger> logCounts = new HashMap<>();

    @Override
    protected void mapNativeValue(
        final GeoWaveInputKey key,
        final Object value,
        final org.apache.hadoop.mapreduce.Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context)
        throws IOException, InterruptedException {
      final AssociationNotification<Object> centroidAssociationFn =
          new AssociationNotification<Object>() {
            @Override
            public void notify(final CentroidPairing<Object> pairing) {
              pairing.getPairedItem().setGroupID(pairing.getCentroid().getID());
              pairing.getPairedItem().setZoomLevel(pairing.getCentroid().getZoomLevel() + 1);
              // just get the contents of the returned ObjectWritable to
              // avoid
              // having to assign outputValWritable rather than update its
              // contents.
              // the 'toWritabeValue' method is efficient, not creating an
              // extra instance of
              // ObjectWritable each time, so this is just a simple
              // exchange of a reference
              outputValWritable.set(
                  toWritableValue(key, pairing.getPairedItem().getWrappedItem()).get());
              AtomicInteger ii = logCounts.get(pairing.getCentroid().getID());

              if (ii == null) {
                ii = new AtomicInteger(0);
                logCounts.put(pairing.getCentroid().getID(), ii);
              }
              ii.incrementAndGet();
            }
          };

      nestedGroupCentroidAssigner.findCentroidForLevel(
          itemWrapperFactory.create(value),
          centroidAssociationFn);

      context.write(key, outputValWritable);
    }

    @Override
    protected void cleanup(final org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException {

      for (final Entry<String, AtomicInteger> e : logCounts.entrySet()) {
        GroupAssignmentMapReduce.LOGGER.info(e.getKey() + " = " + e.getValue());
      }
      super.cleanup(context);
    }

    @Override
    protected void setup(
        final Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context)
        throws IOException, InterruptedException {
      super.setup(context);

      final ScopedJobConfiguration config =
          new ScopedJobConfiguration(
              context.getConfiguration(),
              GroupAssignmentMapReduce.class,
              GroupAssignmentMapReduce.LOGGER);

      try {
        nestedGroupCentroidAssigner =
            new NestedGroupCentroidAssignment<>(
                context,
                GroupAssignmentMapReduce.class,
                GroupAssignmentMapReduce.LOGGER);
      } catch (final Exception e1) {
        throw new IOException(e1);
      }

      try {
        centroidExtractor =
            config.getInstance(
                CentroidParameters.Centroid.EXTRACTOR_CLASS,
                CentroidExtractor.class,
                SimpleFeatureCentroidExtractor.class);
      } catch (final Exception e1) {
        throw new IOException(e1);
      }

      try {
        itemWrapperFactory =
            config.getInstance(
                CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
                AnalyticItemWrapperFactory.class,
                SimpleFeatureItemWrapperFactory.class);

        itemWrapperFactory.initialize(
            context,
            GroupAssignmentMapReduce.class,
            GroupAssignmentMapReduce.LOGGER);
      } catch (final Exception e1) {
        throw new IOException(e1);
      }
    }
  }
}
