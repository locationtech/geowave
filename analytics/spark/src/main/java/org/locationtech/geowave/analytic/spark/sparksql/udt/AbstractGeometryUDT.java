/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.sparksql.udt;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;
import org.locationtech.geowave.core.geotime.util.TWKBReader;
import org.locationtech.geowave.core.geotime.util.TWKBWriter;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

/** Created by jwileczek on 7/20/18. */
public abstract class AbstractGeometryUDT<T extends Geometry> extends UserDefinedType<T> {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public DataType sqlType() {
    return new StructType(
        new StructField[] {new StructField("wkb", DataTypes.BinaryType, true, Metadata.empty())});
  }

  @Override
  public String pyUDT() {
    return "geowave_pyspark.types." + this.getClass().getSimpleName();
  }

  @Override
  public InternalRow serialize(final T obj) {
    final byte[] bytes = new TWKBWriter().write(obj);
    final InternalRow returnRow = new GenericInternalRow(bytes.length);
    returnRow.update(0, bytes);
    return returnRow;
  }

  @Override
  public T deserialize(final Object datum) {
    T geom = null;
    final InternalRow row = (InternalRow) datum;
    final byte[] bytes = row.getBinary(0);
    try {
      geom = (T) new TWKBReader().read(bytes);
    } catch (final ParseException e) {
      e.printStackTrace();
    }
    return geom;
  }
}
