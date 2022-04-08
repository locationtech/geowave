/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial;

import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextExpression;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class TextToSpatialExpression implements SpatialExpression {

  private TextExpression baseExpression;
  private WKTReader wktReader = new WKTReader();

  public TextToSpatialExpression() {}

  public TextToSpatialExpression(final TextExpression baseExpression) {
    this.baseExpression = baseExpression;
  }

  @Override
  public FilterGeometry evaluateValue(Map<String, Object> fieldValues) {
    return evaluateInternal(baseExpression.evaluateValue(fieldValues));
  }

  @Override
  public <T> FilterGeometry evaluateValue(DataTypeAdapter<T> adapter, T entry) {
    return evaluateInternal(baseExpression.evaluateValue(adapter, entry));
  }

  private <T> FilterGeometry evaluateInternal(final String value) {
    if (value != null) {
      try {
        return new UnpreparedFilterGeometry(wktReader.read(value));
      } catch (ParseException e) {
        throw new RuntimeException("Unable to cast text expression to geometry: " + value);
      }
    }
    return null;
  }

  @Override
  public boolean isLiteral() {
    return baseExpression.isLiteral();
  }

  @Override
  public void addReferencedFields(Set<String> fields) {
    baseExpression.addReferencedFields(fields);
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(baseExpression);
  }

  @Override
  public void fromBinary(byte[] bytes) {
    baseExpression = (TextExpression) PersistenceUtils.fromBinary(bytes);
  }

  @Override
  public CoordinateReferenceSystem getCRS(DataTypeAdapter<?> adapter) {
    return GeometryUtils.getDefaultCRS();
  }

}
