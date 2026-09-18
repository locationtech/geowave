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
import com.uber.h3core.PolygonToCellsFlags;
import com.uber.h3core.util.LatLng;
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
    final List<LatLng> coords = h3().cellToBoundary(Lexicoders.LONG.fromByteArray(bin.getBytes()));
    coords.add(coords.get(0));
    return GeometryUtils.GEOMETRY_FACTORY.createPolygon(
        coords.stream().map(latLng -> new Coordinate(latLng.lng, latLng.lat)).toArray(
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

    // Minimal buffer distance in degrees (~0.1mm at the equator).
    // This is only needed to convert a LineString to a valid polygon for the H3 API,
    // not for geometric coverage purposes.
    private static final double MINIMAL_BUFFER_DEGREES = 1e-9;

    public H3GeometryHandler(final int precision) {
      super();
      this.precision = precision;
    }

    @Override
    public void handlePoint(final Point point) {
      ids.add(h3().latLngToCell(point.getY(), point.getX(), precision));
    }

    @Override
    public void handleLineString(final LineString lineString) {
      // polygonToCellsExperimental needs a 2D polygon, so the line is buffered by a negligible
      // amount (~0.1mm at the equator) purely to make one. With containment_overlapping every cell
      // the buffered shape touches is returned, which for a buffer this small is exactly the set of
      // cells the line itself passes through.
      final Coordinate[] coords = lineString.getCoordinates();
      if (coords.length == 1) {
        handlePoint(lineString.getPointN(0));
        return;
      }
      final H3GeometryHandler handler = new H3GeometryHandler(precision);
      GeometryUtils.visitGeometry(lineString.buffer(MINIMAL_BUFFER_DEGREES), handler);
      ids.addAll(handler.ids);
    }

    @Override
    public void handlePolygon(final Polygon polygon) {
      // Using polygonToCellsExperimental with CONTAINMENT_OVERLAPPING mode to get all hexagons
      // that intersect the polygon at any point. This eliminates the need for the buffering
      // workaround that was previously required with polygonToCells (which only returns hexagons
      // whose centers are inside the polygon).
      final int numInteriorRings = polygon.getNumInteriorRing();
      final List<Long> idsToAdd;
      if (numInteriorRings > 0) {
        final List<List<LatLng>> holes = new ArrayList<>(numInteriorRings);
        for (int i = 0; i < numInteriorRings; i++) {
          holes.add(
              Arrays.stream(polygon.getInteriorRingN(i).getCoordinates()).map(
                  c -> new LatLng(c.getY(), c.getX())).collect(Collectors.toList()));
        }
        idsToAdd =
            h3().polygonToCellsExperimental(
                Arrays.stream(polygon.getExteriorRing().getCoordinates()).map(
                    c -> new LatLng(c.getY(), c.getX())).collect(Collectors.toList()),
                holes,
                precision,
                PolygonToCellsFlags.containment_overlapping);

      } else {
        idsToAdd =
            h3().polygonToCellsExperimental(
                Arrays.stream(polygon.getExteriorRing().getCoordinates()).map(
                    c -> new LatLng(c.getY(), c.getX())).collect(Collectors.toList()),
                null,
                precision,
                PolygonToCellsFlags.containment_overlapping);
      }
      if (idsToAdd.isEmpty()) {
        // For very small polygons that don't intersect any hexagon centers, fall back to centroid
        handlePoint(polygon.getCentroid());
      } else {
        ids.addAll(idsToAdd);
      }
    }
  }
}
