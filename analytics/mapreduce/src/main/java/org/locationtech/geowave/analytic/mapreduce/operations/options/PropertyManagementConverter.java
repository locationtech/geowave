/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.operations.options;

import java.lang.reflect.AnnotatedElement;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.annotations.CentroidParameter;
import org.locationtech.geowave.analytic.param.annotations.ClusteringParameter;
import org.locationtech.geowave.analytic.param.annotations.CommonParameter;
import org.locationtech.geowave.analytic.param.annotations.ExtractParameter;
import org.locationtech.geowave.analytic.param.annotations.GlobalParameter;
import org.locationtech.geowave.analytic.param.annotations.HullParameter;
import org.locationtech.geowave.analytic.param.annotations.InputParameter;
import org.locationtech.geowave.analytic.param.annotations.JumpParameter;
import org.locationtech.geowave.analytic.param.annotations.MapReduceParameter;
import org.locationtech.geowave.analytic.param.annotations.OutputParameter;
import org.locationtech.geowave.analytic.param.annotations.PartitionParameter;
import org.locationtech.geowave.analytic.param.annotations.SampleParameter;
import org.locationtech.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import org.locationtech.geowave.core.cli.prefix.JCommanderTranslationMap;
import org.locationtech.geowave.core.cli.prefix.TranslationEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a stop-gap measure which allows us to copy parameters read from the command line into the
 * PropertyManagement object.
 */
public class PropertyManagementConverter {
  static final Logger LOGGER = LoggerFactory.getLogger(PropertyManagementConverter.class);

  final PropertyManagement properties;

  public PropertyManagementConverter(final PropertyManagement properties) {
    this.properties = properties;
  }

  public PropertyManagement getProperties() {
    return properties;
  }

  /**
   * Find annotations in the object, and copy the values to the PropertyManagement
   *
   * @param object
   */
  public void readProperties(final Object object) {
    final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
    translator.addObject(object);
    final JCommanderTranslationMap map = translator.translate();
    for (final TranslationEntry entry : map.getEntries().values()) {
      // Has annotation?
      final AnnotatedElement element = entry.getMember();
      final CentroidParameter centroid = element.getAnnotation(CentroidParameter.class);
      final ClusteringParameter clustering = element.getAnnotation(ClusteringParameter.class);
      final CommonParameter common = element.getAnnotation(CommonParameter.class);
      final ExtractParameter extract = element.getAnnotation(ExtractParameter.class);
      final GlobalParameter global = element.getAnnotation(GlobalParameter.class);
      final HullParameter hull = element.getAnnotation(HullParameter.class);
      final InputParameter input = element.getAnnotation(InputParameter.class);
      final JumpParameter jump = element.getAnnotation(JumpParameter.class);
      final MapReduceParameter mapReduce = element.getAnnotation(MapReduceParameter.class);
      final OutputParameter output = element.getAnnotation(OutputParameter.class);
      final PartitionParameter partition = element.getAnnotation(PartitionParameter.class);
      final SampleParameter sample = element.getAnnotation(SampleParameter.class);

      if (centroid != null) {
        handleEnum(entry, centroid.value());
      }
      if (clustering != null) {
        handleEnum(entry, clustering.value());
      }
      if (common != null) {
        handleEnum(entry, common.value());
      }
      if (extract != null) {
        handleEnum(entry, extract.value());
      }
      if (global != null) {
        handleEnum(entry, global.value());
      }
      if (hull != null) {
        handleEnum(entry, hull.value());
      }
      if (input != null) {
        handleEnum(entry, input.value());
      }
      if (jump != null) {
        handleEnum(entry, jump.value());
      }
      if (mapReduce != null) {
        handleEnum(entry, mapReduce.value());
      }
      if (output != null) {
        handleEnum(entry, output.value());
      }
      if (partition != null) {
        handleEnum(entry, partition.value());
      }
      if (sample != null) {
        handleEnum(entry, sample.value());
      }
    }
  }

  /**
   * For a single value, copy the value from the object to PropertyManagement.
   *
   * @param entry
   * @param enumVal
   */
  @SuppressWarnings("unchecked")
  private void handleEnum(final TranslationEntry entry, final ParameterEnum<?>[] enumVals) {
    final Object value = entry.getParam().get(entry.getObject());
    if (value != null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            String.format(
                "Analytic Property Value: %s = %s",
                entry.getAsPropertyName(),
                value.toString()));
      }
      for (final ParameterEnum<?> enumVal : enumVals) {
        ((ParameterEnum<Object>) enumVal).getHelper().setValue(properties, value);
      }
    }
  }
}
