/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.util;

public class TWKBUtils {
  public static final byte POINT_TYPE = 1;
  public static final byte LINESTRING_TYPE = 2;
  public static final byte POLYGON_TYPE = 3;
  public static final byte MULTIPOINT_TYPE = 4;
  public static final byte MULTILINESTRING_TYPE = 5;
  public static final byte MULTIPOLYGON_TYPE = 6;
  public static final byte GEOMETRYCOLLECTION_TYPE = 7;

  public static final byte EXTENDED_DIMENSIONS = 1 << 3;
  public static final byte EMPTY_GEOMETRY = 1 << 4;

  public static final byte MAX_COORD_PRECISION = 7;
  public static final byte MIN_COORD_PRECISION = -8;

  public static final byte MAX_EXTENDED_PRECISION = 3;
  public static final byte MIN_EXTENDED_PRECISION = -4;

  public static int zigZagEncode(final int value) {
    return (value << 1) ^ (value >> 31);
  }

  public static int zigZagDecode(final int value) {
    final int temp = (((value << 31) >> 31) ^ value) >> 1;
    return temp ^ (value & (1 << 31));
  }
}
