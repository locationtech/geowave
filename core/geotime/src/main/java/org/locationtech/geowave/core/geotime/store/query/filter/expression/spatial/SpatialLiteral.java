/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial;

import java.nio.ByteBuffer;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.InvalidFilterException;
import org.locationtech.geowave.core.store.query.filter.expression.Literal;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * A spatial implementation of literal, representing spatial (geometric) literal objects.
 */
public class SpatialLiteral extends Literal<FilterGeometry> implements SpatialExpression {

  private CoordinateReferenceSystem crs;

  public SpatialLiteral() {}

  public SpatialLiteral(final FilterGeometry literal) {
    super(literal);
    crs = GeometryUtils.getDefaultCRS();
  }

  public SpatialLiteral(final FilterGeometry literal, final CoordinateReferenceSystem crs) {
    super(literal);
    this.crs = crs;
  }

  @Override
  public CoordinateReferenceSystem getCRS(final DataTypeAdapter<?> adapter) {
    return crs;
  }

  /**
   * Prepare this literal by converting it to the provided coordinate reference system and preparing
   * the geometry.
   * 
   * @param targetCRS the target coordinate reference system of the geometry
   */
  public void prepare(final CoordinateReferenceSystem targetCRS) {
    if ((literal != null) && (literal instanceof UnpreparedFilterGeometry)) {
      try {
        final Geometry transformed =
            GeometryUtils.crsTransform(
                literal.getGeometry(),
                CRS.findMathTransform(crs, targetCRS));
        literal =
            new PreparedFilterGeometry(GeometryUtils.PREPARED_GEOMETRY_FACTORY.create(transformed));
        crs = targetCRS;
      } catch (final FactoryException e) {
        throw new RuntimeException("Unable to transform spatial literal", e);
      }
    }
  }

  private static FilterGeometry toGeometry(final Object literal) {
    final Geometry geometry;
    if (literal == null) {
      return null;
    }
    if (literal instanceof Geometry) {
      geometry = (Geometry) literal;
    } else if (literal instanceof Envelope) {
      geometry = GeometryUtils.GEOMETRY_FACTORY.toGeometry((Envelope) literal);
    } else if (literal instanceof String) {
      try {
        geometry = new WKTReader().read((String) literal);
      } catch (ParseException e) {
        throw new InvalidFilterException("Unable to parse well-known text geometry", e);
      }
    } else {
      throw new InvalidFilterException("Invalid spatial literal: " + literal.getClass().getName());
    }
    return new UnpreparedFilterGeometry(geometry);
  }

  public static SpatialLiteral of(final Object literal) {
    final CoordinateReferenceSystem crs;
    if (literal instanceof ReferencedEnvelope) {
      crs = ((ReferencedEnvelope) literal).getCoordinateReferenceSystem();
    } else {
      crs = GeometryUtils.getDefaultCRS();
    }
    return of(literal, crs);
  }

  public static SpatialLiteral of(Object literal, final CoordinateReferenceSystem crs) {
    if (literal == null) {
      return new SpatialLiteral(null);
    }
    if (literal instanceof SpatialLiteral) {
      return (SpatialLiteral) literal;
    }
    if (literal instanceof Expression && ((Expression<?>) literal).isLiteral()) {
      literal = ((Expression<?>) literal).evaluateValue(null);
    }
    return new SpatialLiteral(toGeometry(literal), crs);
  }

  @Override
  public String toString() {
    return literal.getGeometry().toText();
  }

  @Override
  public byte[] toBinary() {
    if (literal == null) {
      return new byte[] {(byte) 0};
    }
    final byte[] crsBytes = StringUtils.stringToBinary(crs.toWKT());
    final byte[] geometryBytes = PersistenceUtils.toBinary(literal);
    final ByteBuffer buffer =
        ByteBuffer.allocate(
            1
                + VarintUtils.unsignedIntByteLength(crsBytes.length)
                + VarintUtils.unsignedIntByteLength(geometryBytes.length)
                + crsBytes.length
                + geometryBytes.length);
    buffer.put((byte) 1);
    VarintUtils.writeUnsignedInt(crsBytes.length, buffer);
    buffer.put(crsBytes);
    VarintUtils.writeUnsignedInt(geometryBytes.length, buffer);
    buffer.put(geometryBytes);
    return buffer.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final byte nullByte = buffer.get();
    if (nullByte == 0) {
      literal = null;
      return;
    }
    final byte[] crsBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(crsBytes);
    final byte[] geometryBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(geometryBytes);
    try {
      crs = CRS.parseWKT(StringUtils.stringFromBinary(crsBytes));
    } catch (final FactoryException e) {
      throw new RuntimeException("Unable to parse CRS from spatial literal.");
    }
    literal = (FilterGeometry) PersistenceUtils.fromBinary(geometryBytes);
  }

}
