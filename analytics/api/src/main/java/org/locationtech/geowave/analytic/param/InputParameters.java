/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.param;

import org.apache.hadoop.fs.Path;

public class InputParameters {
  public enum Input implements ParameterEnum<Object> {
    INPUT_FORMAT(FormatConfiguration.class, "ifc", "Input Format Class", true, true),
    HDFS_INPUT_PATH(Path.class, "iip", "Input HDFS File Path", false, true);

    private final ParameterHelper<Object> helper;

    private Input(
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
    public ParameterHelper<Object> getHelper() {
      return helper;
    }
  }
}
