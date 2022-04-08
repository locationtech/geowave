/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.binning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.GeometryUtils.GeometryHandler;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;
import com.uber.h3core.exceptions.LineUndefinedException;
import com.uber.h3core.util.GeoCoord;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

class H3BinningHelper implements SpatialBinningHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(H3BinningHelper.class);
  private static final Object H3_MUTEX = new Object();
  private static H3Core h3Core;

  @Override
  public ByteArray[] getSpatialBins(final Geometry geometry, final int precision) {
    final H3GeometryHandler h3Handler = new H3GeometryHandler(precision);
    GeometryUtils.visitGeometry(geometry, h3Handler);
    return h3Handler.ids.stream().map(Lexicoders.LONG::toByteArray).map(ByteArray::new).toArray(
        ByteArray[]::new);
  }

  @Override
  public Geometry getBinGeometry(final ByteArray bin, final int precision) {
    // understanding is that this does not produce a closed loop so we need to add the first point
    // at the end to close the loop
    final List<GeoCoord> coords =
        h3().h3ToGeoBoundary(Lexicoders.LONG.fromByteArray(bin.getBytes()));
    coords.add(coords.get(0));
    return GeometryUtils.GEOMETRY_FACTORY.createPolygon(
        coords.stream().map(geoCoord -> new Coordinate(geoCoord.lng, geoCoord.lat)).toArray(
            Coordinate[]::new));
  }

  @Override
  public String binToString(final byte[] binId) {
    return h3().h3ToString(Lexicoders.LONG.fromByteArray(binId));
  }

  @Override
  public int getBinByteLength(final int precision) {
    return Long.BYTES;
  }

  @SuppressFBWarnings
  private static H3Core h3() {
    if (h3Core == null) {
      synchronized (H3_MUTEX) {
        if (h3Core == null) {
          try {
            h3Core = H3Core.newInstance();
          } catch (final IOException e) {
            LOGGER.error("Unable to load native H3 libraries", e);
          }
        }
      }
    }
    return h3Core;
  }

  private static class H3GeometryHandler implements GeometryHandler {
    private final int precision;
    private final Set<Long> ids = new HashSet<>();
    // this is just an approximation
    private static final double KM_PER_DEGREE = 111;
    private final boolean hasBeenBuffered;

    public H3GeometryHandler(final int precision) {
      this(precision, false);
    }

    public H3GeometryHandler(final int precision, final boolean hasBeenBuffered) {
      super();
      this.precision = precision;
      this.hasBeenBuffered = hasBeenBuffered;
    }

    @Override
    public void handlePoint(final Point point) {
      ids.add(h3().geoToH3(point.getY(), point.getX(), precision));
    }

    private Long coordToH3(final Coordinate coord) {
      return h3().geoToH3(coord.getY(), coord.getX(), precision);
    }

    @Override
    public void handleLineString(final LineString lineString) {
      final double edgeLengthDegrees = h3().edgeLength(precision, LengthUnit.km) / KM_PER_DEGREE;
      internalHandlePolygon((Polygon) lineString.buffer(edgeLengthDegrees));

      // this is an under-approximation, but turns out just as poor of an approximation as the above
      // logic and should be much faster (doing both actually improves accuracy a bit, albeit more
      // expensive)
      final Coordinate[] coords = lineString.getCoordinates();
      if (coords.length > 1) {
        Coordinate prev = coords[0];
        for (int i = 1; i < coords.length; i++) {
          try {
            ids.addAll(h3().h3Line(coordToH3(prev), coordToH3(coords[i])));
          } catch (final LineUndefinedException e) {
            LOGGER.error("Unable to add H3 line for " + lineString, e);
          }
          prev = coords[i];
        }
      } else if (coords.length == 1) {
        ids.add(coordToH3(coords[0]));
      }
    }

    private void internalHandlePolygon(final Polygon polygon) {
      final int numInteriorRings = polygon.getNumInteriorRing();
      final List<Long> idsToAdd;
      if (numInteriorRings > 0) {
        final List<List<GeoCoord>> holes = new ArrayList<>(numInteriorRings);
        for (int i = 0; i < numInteriorRings; i++) {
          holes.add(
              Arrays.stream(polygon.getInteriorRingN(i).getCoordinates()).map(
                  c -> new GeoCoord(c.getY(), c.getX())).collect(Collectors.toList()));
        }
        idsToAdd =
            h3().polyfill(
                Arrays.stream(polygon.getExteriorRing().getCoordinates()).map(
                    c -> new GeoCoord(c.getY(), c.getX())).collect(Collectors.toList()),
                holes,
                precision);

      } else {
        idsToAdd =
            h3().polyfill(
                Arrays.stream(polygon.getExteriorRing().getCoordinates()).map(
                    c -> new GeoCoord(c.getY(), c.getX())).collect(Collectors.toList()),
                null,
                precision);
      }
      if (idsToAdd.isEmpty()) {
        // given the approximations involved with H3 this is still a slight possibility, even given
        // our geometric buffering to circumvent the approximations
        handlePoint(polygon.getCentroid());
      } else {
        ids.addAll(idsToAdd);
      }
    }

    @Override
    public void handlePolygon(final Polygon polygon) {
      // the H3 APIs is an under-approximation - it only returns hexagons whose center is inside the
      // polygon, *not* all hexagons that intersect the polygon
      // by buffering the polygon by the approximation of the edge length we can at least get closer
      // to all the intersections
      if (hasBeenBuffered) {
        internalHandlePolygon(polygon);
      } else {
        final double edgeLengthDegrees = h3().edgeLength(precision, LengthUnit.km) / KM_PER_DEGREE;
        final H3GeometryHandler handler = new H3GeometryHandler(precision, true);
        GeometryUtils.visitGeometry(polygon.buffer(edgeLengthDegrees), handler);
        ids.addAll(handler.ids);
      }
    }
  }
}
