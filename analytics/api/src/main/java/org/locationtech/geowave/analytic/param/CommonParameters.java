/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.param;

import org.locationtech.geowave.analytic.distance.DistanceFn;
import org.locationtech.geowave.analytic.extract.DimensionExtractor;
import org.locationtech.geowave.analytic.model.IndexModelBuilder;

public class CommonParameters {
  public enum Common implements ParameterEnum {
    DIMENSION_EXTRACT_CLASS(DimensionExtractor.class, "dde",
        "Dimension Extractor Class implements org.locationtech.geowave.analytics.extract.DimensionExtractor",
        true, true),
    DISTANCE_FUNCTION_CLASS(DistanceFn.class, "cdf",
        "Distance Function Class implements org.locationtech.geowave.analytics.distance.DistanceFn",
        true, true),
    INDEX_MODEL_BUILDER_CLASS(IndexModelBuilder.class, "cim",
        "Class implements org.locationtech.geowave.analytics.tools.model.IndexModelBuilder", true,
        true);

    private final ParameterHelper<?> helper;

    Common(
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
