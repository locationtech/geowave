/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.param;

import org.locationtech.geowave.analytic.AnalyticItemWrapperFactory;
import org.locationtech.geowave.analytic.Projection;
import org.locationtech.geowave.analytic.extract.CentroidExtractor;

public class HullParameters {
  public enum Hull implements ParameterEnum {
    INDEX_NAME(String.class, "hid", "Index Identifier for Centroids", false, true),
    DATA_TYPE_ID(String.class, "hdt", "Data Type ID for a centroid item", false, true),
    DATA_NAMESPACE_URI(String.class, "hns", "Data Type Namespace for a centroid item", false, true),
    REDUCER_COUNT(Integer.class, "hrc", "Centroid Reducer Count", false, true),
    PROJECTION_CLASS(Projection.class, "hpe",
        "Class to project on to 2D space. Implements org.locationtech.geowave.analytics.tools.Projection",
        true, true),
    EXTRACTOR_CLASS(CentroidExtractor.class, "hce",
        "Centroid Exractor Class implements org.locationtech.geowave.analytics.extract.CentroidExtractor",
        true, true),
    WRAPPER_FACTORY_CLASS(AnalyticItemWrapperFactory.class, "hfc",
        "Class to create analytic item to capture hulls. Implements org.locationtech.geowave.analytics.tools.AnalyticItemWrapperFactory",
        true, true),
    ITERATION(Integer.class, "hi", "The iteration of the hull calculation", false, true),
    HULL_BUILDER(Projection.class, "hhb", "Hull Builder", true, true),
    ZOOM_LEVEL(Integer.class, "hzl", "Zoom Level Number", false, true);

    private final ParameterHelper<?> helper;

    private Hull(
        final Class baseClass,
        final String name,
        final String description,
        final boolean isClass,
        final boolean hasArg) {
      helper = new BasicParameterHelper(this, baseClass, name, description, isClass, hasArg);
    }

    @Override
    public Enum<?> self() {
      return this;
    }

    @Override
    public ParameterHelper<?> getHelper() {
      return helper;
    }
  }
}
