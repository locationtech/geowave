/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.field;

import javax.annotation.Nullable;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.jts.geom.Geometry;

public class GeometrySerializationProvider implements FieldSerializationProviderSpi<Geometry> {
  private Integer geometryPrecision;

  public GeometrySerializationProvider() {
    geometryPrecision = GeometryUtils.MAX_GEOMETRY_PRECISION;
  }

  public GeometrySerializationProvider(@Nullable final Integer geometryPrecision) {
    super();
    this.geometryPrecision = geometryPrecision;
  }

  @Override
  public FieldReader<Geometry> getFieldReader() {
    return new GeometryReader(geometryPrecision);
  }

  @Override
  public FieldWriter<Geometry> getFieldWriter() {
    return new GeometryWriter(geometryPrecision);
  }

  protected static class GeometryReader implements FieldReader<Geometry> {
    private Integer geometryPrecision;

    public GeometryReader() {
      geometryPrecision = GeometryUtils.MAX_GEOMETRY_PRECISION;
    }

    public GeometryReader(@Nullable final Integer geometryPrecision) {
      this.geometryPrecision = geometryPrecision;
    }

    public void setPrecision(@Nullable final Integer geometryPrecision) {
      this.geometryPrecision = geometryPrecision;
    }

    @Override
    public Geometry readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length < 1)) {
        return null;
      }
      return GeometryUtils.geometryFromBinary(
          fieldData,
          geometryPrecision,
          FieldUtils.SERIALIZATION_VERSION);
    }

    @Override
    public Geometry readField(final byte[] fieldData, final byte serializationVersion) {
      if ((fieldData == null) || (fieldData.length < 1)) {
        return null;
      }
      return GeometryUtils.geometryFromBinary(fieldData, geometryPrecision, serializationVersion);
    }
  }

  protected static class GeometryWriter implements FieldWriter<Geometry> {
    private Integer geometryPrecision;

    public GeometryWriter() {
      geometryPrecision = GeometryUtils.MAX_GEOMETRY_PRECISION;
    }

    public GeometryWriter(@Nullable final Integer geometryPrecision) {
      this.geometryPrecision = geometryPrecision;
    }

    public void setPrecision(@Nullable final Integer geometryPrecision) {
      this.geometryPrecision = geometryPrecision;
    }

    @Override
    public byte[] writeField(final Geometry fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }
      return GeometryUtils.geometryToBinary(fieldValue, geometryPrecision);
    }
  }
}
