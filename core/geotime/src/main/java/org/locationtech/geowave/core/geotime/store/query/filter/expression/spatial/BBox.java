/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial;

import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.jts.geom.Envelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * Predicate that passes when the first operand is within the bounding box of the second operand.
 */
public class BBox extends Intersects {

  public BBox() {}

  public BBox(
      final SpatialExpression expression,
      final double minX,
      final double minY,
      final double maxX,
      final double maxY,
      final boolean loose) {
    this(expression, minX, minY, maxX, maxY, null, loose);
  }

  public BBox(
      final SpatialExpression expression,
      final double minX,
      final double minY,
      final double maxX,
      final double maxY,
      final CoordinateReferenceSystem crs,
      final boolean loose) {
    super(
        expression,
        SpatialLiteral.of(
            new ReferencedEnvelope(
                minX,
                maxX,
                minY,
                maxY,
                crs == null ? GeometryUtils.getDefaultCRS() : crs)),
        loose);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(loose ? "BBOXLOOSE(" : "BBOX(");
    final Envelope envelope = expression2.evaluateValue(null).getGeometry().getEnvelopeInternal();
    sb.append(expression1.toString());
    sb.append(",");
    sb.append(envelope.getMinX());
    sb.append(",");
    sb.append(envelope.getMinY());
    sb.append(",");
    sb.append(envelope.getMaxX());
    sb.append(",");
    sb.append(envelope.getMaxY());
    sb.append(")");
    return sb.toString();
  }

}
