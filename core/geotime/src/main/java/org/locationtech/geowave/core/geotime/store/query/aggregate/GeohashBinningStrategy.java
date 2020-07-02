/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.store.query.aggregate.AggregationBinningStrategy;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import java.nio.ByteBuffer;

/**
 * This strategy uses the geohash of the types geometry to bin data.
 *
 * @param <T> The type of the entry. The geometry inside of it is queried, and the geohash of that
 *        geometry is used as the bin.
 */
public abstract class GeohashBinningStrategy<T> implements AggregationBinningStrategy<T> {

  /**
   * The geohash precision.
   */
  protected int precision;

  /**
   * Use the given precision to bin objects
   * 
   * @param precision The Geohash precision to calculate bins.
   */
  public GeohashBinningStrategy(int precision) {
    this.precision = precision;
  }

  /**
   * Extract the geometry from the entry.
   * 
   * @param entry The entry that will be binned using this strategy.
   * @return The geometry object in the entry, or null if no geometry is found.
   */
  abstract Geometry getGeometry(T entry);

  /**
   * Computes the geoHash of a given geometry.
   *
   * @param geo The geometry to get the geohash of. Note that the centroid of the geometry is used
   *        for the computation.
   * @param precision The precision to compute the geohash with.
   * @return A String of length precision that represents the Geohash of the given geometry.
   */
  protected static String encodeToGeohash(Geometry geo, int precision) {
    final String GEOHASH_BASE_32 = "0123456789bcdefghjkmnpqrstuvwxyz";

    Point centroid = geo.getCentroid();
    double latitude = centroid.getX();
    double longitude = centroid.getY();

    // index of char to use in GEOHASH_BASE_32
    int idx = 0;
    // fill up all 5 bits (of 32-char (2^5 == 32) geohash string) before querying.
    int bit = 0;
    boolean evenBit = true;
    // start at edges, and refine as we go further on.
    double latitudeMin = -90;
    double latitudeMax = 90;
    double longitudeMin = -180;
    double longitudeMax = 180;

    StringBuilder geohash = new StringBuilder(precision);
    while (geohash.length() < precision) {
      if (evenBit) {
        final double currentLongitudeMid = (longitudeMin + longitudeMax) / 2;
        if (longitude >= currentLongitudeMid) {
          idx = idx * 2 + 1;
          longitudeMin = currentLongitudeMid;
        } else {
          idx *= 2;
          longitudeMax = currentLongitudeMid;
        }
      } else {
        final double currentLatitudeMid = (latitudeMin + latitudeMax) / 2;
        if (latitude >= currentLatitudeMid) {
          idx = idx * 2 + 1;
          latitudeMin = currentLatitudeMid;
        } else {
          idx *= 2;
          latitudeMax = currentLatitudeMid;
        }
      }
      evenBit = !evenBit;

      if (++bit == 5) {
        // 2^5 bits = 32 bits, which is our idx in the char-map.
        geohash.append(GEOHASH_BASE_32.charAt(idx));
        idx = 0;
        bit = 0;
      }
    }
    return geohash.toString();
  }

  /**
   * @return The precision that is used when calculating bins for entries.
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * calculates appropriate bins for a given entry. GeohashBinningStrategy only ever bins into
   * singleton-arrays.
   * 
   * @param entry An entry to bin, utilizing its' geohash.
   * @return a length-1 array of the bin that this entry can be placed into. `null` if no Geometry
   *         was found in the entry.
   */
  @Override
  public String[] binEntry(T entry) {
    Geometry geometry = this.getGeometry(entry);
    return geometry == null ? null : new String[] {encodeToGeohash(geometry, this.precision)};
  }

  @Override
  public byte[] toBinary() {
    return ByteBuffer.allocate(4).putInt(this.precision).array();
  }

  @Override
  public void fromBinary(byte[] bytes) {
    this.precision = ByteBuffer.wrap(bytes).getInt();
  }
}
