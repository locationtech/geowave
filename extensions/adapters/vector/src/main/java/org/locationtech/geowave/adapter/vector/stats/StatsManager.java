/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.geotime.store.statistics.FeatureBoundingBoxStatistics;
import org.locationtech.geowave.core.geotime.store.statistics.FeatureTimeRangeStatistics;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DefaultFieldStatisticVisibility;
import org.locationtech.geowave.core.store.adapter.statistics.FieldNameStatisticVisibility;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Object that manages statistics for an adapter */
public class StatsManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatsManager.class);

  /** Visibility that can be used within GeoWave as a CommonIndexValue */
  private static final EntryVisibilityHandler<SimpleFeature> DEFAULT_VISIBILITY_HANDLER =
      new DefaultFieldStatisticVisibility<>();

  /** List of stats objects supported by this manager for the adapter */
  private final List<InternalDataStatistics<SimpleFeature, ?, ?>> statsObjList = new ArrayList<>();
  /** List of visibility handlers supported by this manager for the stats objects */
  private final Map<StatisticsId, String> statisticsIdToFieldNameMap = new HashMap<>();

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Constructor - Creates a StatsManager that supports the specified adapter with a persisted
   * SimpleFeatureType
   *
   * @param dataAdapter - adapter to be associated with this manager
   * @param persistedType - the feature type to be associated with the given adapter
   */
  public StatsManager(
      final DataTypeAdapter<SimpleFeature> dataAdapter,
      final SimpleFeatureType persistedType) {
    this(dataAdapter, persistedType, null, null);
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Constructor - Creates a StatsManager that supports the specified adapter with a persisted and
   * re-projected SimpleFeatureType that is translated by the transform
   *
   * @param dataAdapter - adapter to be associated with this manager
   * @param persistedType - the feature type to be associated with the given adapter
   * @param reprojectedType - feature type re-projected with new CRS. Used if stats object is a
   *        Feature Bounding Box.
   * @param transform - type of transform applied to persisted type. Used if stats object is a
   *        Feature Bounding Box.
   */
  public StatsManager(
      final DataTypeAdapter<SimpleFeature> dataAdapter,
      final SimpleFeatureType persistedType,
      final SimpleFeatureType reprojectedType,
      final MathTransform transform) {
    // For all the attribute descriptors listed in the persisted
    // featuretype,
    // go through and look for stats that can be tracked against this
    // attribute type

    for (final AttributeDescriptor descriptor : persistedType.getAttributeDescriptors()) {

      // ---------------------------------------------------------------------
      // For temporal and geometry because there is a dependency on these
      // stats for optimizations within the GeoServer adapter.

      if (TimeUtils.isTemporal(descriptor.getType().getBinding())) {

        addStats(
            new FeatureTimeRangeStatistics(descriptor.getLocalName()),
            descriptor.getLocalName());
      } else if (Geometry.class.isAssignableFrom(descriptor.getType().getBinding())) {
        addStats(
            new FeatureBoundingBoxStatistics(descriptor.getLocalName(), reprojectedType, transform),
            descriptor.getLocalName());
      }

      // ---------------------------------------------------------------------
      // If there is a stats field in the feature, then that is a
      // StatsConfigurationCollection and the stats objects for the
      // feature need to be extracted and added as stats for this adapter

      if (descriptor.getUserData().containsKey("stats")) {
        final StatsConfigurationCollection statsConfigurations =
            (StatsConfigurationCollection) descriptor.getUserData().get("stats");

        final List<StatsConfig<SimpleFeature>> featureConfigs =
            statsConfigurations.getConfigurationsForAttribute();

        for (final StatsConfig<SimpleFeature> statConfig : featureConfigs) {
          addStats(statConfig.create(null, descriptor.getLocalName()), descriptor.getLocalName());
        }

      }

      // ---------------------------------------------------------------------
      // If this numeric, then add a stats object that relate

      else if (Number.class.isAssignableFrom(descriptor.getType().getBinding())) {
        addStats(
            new FeatureNumericRangeStatistics(descriptor.getLocalName()),
            descriptor.getLocalName());
      }
    }
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Creates a stats object to be tracked for this adapter based on type
   *
   * @param statisticsId - name of statistics type to be created
   * @return new statistics object of specified type
   */
  public InternalDataStatistics<SimpleFeature, ?, ?> createDataStatistics(
      final StatisticsId statisticsId) {
    for (final InternalDataStatistics<SimpleFeature, ?, ?> statObj : statsObjList) {
      if (statObj.getType().equals(statisticsId.getType())
          && statObj.getExtendedId().equals(statisticsId.getExtendedId())) {
        // TODO most of the data statistics seem to do shallow clones
        // that pass along a lot of references - this seems
        // counter-intuitive to the spirit of a "create" method, but it
        // seems to work right now?
        return ((AbstractDataStatistics<SimpleFeature, ?, ?>) statObj).duplicate();
      }
    }

    if (statisticsId.getType().equals(CountDataStatistics.STATS_TYPE)) {
      return new CountDataStatistics<>();
    }

    // HP Fortify "Log Forging" false positive
    // What Fortify considers "user input" comes only
    // from users with OS-level access anyway

    LOGGER.warn(
        "Unrecognized statistics ID of type '"
            + statisticsId.getType().getString()
            + "' with field name '"
            + statisticsId.getExtendedId()
            + "', using count statistic.");
    return new CountDataStatistics<>();
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Get a visibility handler for the specified statistics object
   *
   * @param statisticsId
   * @return - visibility handler for given stats object
   */
  public EntryVisibilityHandler<SimpleFeature> getVisibilityHandler(
      final CommonIndexModel indexModel,
      final DataTypeAdapter<SimpleFeature> adapter,
      final StatisticsId statisticsId) {
    // If the statistics object is of type CountDataStats or there is no
    // visibility handler, then return the default visibility handler
    if (statisticsId.getType().equals(CountDataStatistics.STATS_TYPE)
        || (!statisticsIdToFieldNameMap.containsKey(statisticsId))) {
      return DEFAULT_VISIBILITY_HANDLER;
    }

    final String fieldName = statisticsIdToFieldNameMap.get(statisticsId);
    return new FieldNameStatisticVisibility<>(fieldName, indexModel, adapter);
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Adds/Replaces a stats object for the given adapter <br> Supports object replacement.
   *
   * @param statsObj - data stats object to be tracked by adding or replacement
   * @param visibilityHandler - type of visibility required to access the stats object
   */
  public void addStats(
      final InternalDataStatistics<SimpleFeature, ?, ?> statsObj,
      final String fieldName) {
    int replaceStat = 0;

    // Go through stats list managed by this manager and look for a match
    for (final InternalDataStatistics<SimpleFeature, ?, ?> currentStat : statsObjList) {
      if (currentStat.getType().equals(statsObj.getType())
          && currentStat.getExtendedId().equals(statsObj.getExtendedId())) {
        // If a match was found for an existing stat object in list,
        // remove it now and replace it later.
        statsObjList.remove(replaceStat);
        break;
      }
      replaceStat++; // Not found, check next stat object
    }

    statsObjList.add(statsObj);
    statisticsIdToFieldNameMap.put(
        new StatisticsId(statsObj.getType(), statsObj.getExtendedId()),
        fieldName);
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Get an array of stats object IDs for the Stats Manager
   *
   * @return Array of stats object IDs as 'ByteArrayId'
   */
  public StatisticsId[] getSupportedStatistics() {
    // Why are we adding a CountDataStatistics??

    final StatisticsId[] statObjIds = new StatisticsId[statsObjList.size() + 1];
    int i = 0;

    for (final InternalDataStatistics<SimpleFeature, ?, ?> statObj : statsObjList) {
      statObjIds[i++] = new StatisticsId(statObj.getType(), statObj.getExtendedId());
    }

    statObjIds[i] = CountDataStatistics.STATS_TYPE.newBuilder().build().getId();

    return statObjIds;
  }
}
