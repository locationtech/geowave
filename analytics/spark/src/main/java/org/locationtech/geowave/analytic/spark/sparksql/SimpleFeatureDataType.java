/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.sparksql;

import org.apache.spark.sql.types.DataType;

public class SimpleFeatureDataType {
  private final DataType dataType;
  private final boolean isGeom;

  public SimpleFeatureDataType(DataType dataType, boolean isGeom) {
    this.dataType = dataType;
    this.isGeom = isGeom;
  }

  public DataType getDataType() {
    return dataType;
  }

  public boolean isGeom() {
    return isGeom;
  }
}
