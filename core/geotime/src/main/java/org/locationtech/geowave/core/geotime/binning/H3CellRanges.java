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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.GeometryUtils.GeometryHandler;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.geom.util.PolygonExtracter;
import org.locationtech.jts.operation.overlayng.OverlayNG;
import org.locationtech.jts.operation.overlayng.OverlayNGRobust;
import com.google.common.primitives.Longs;
import com.uber.h3core.H3Core;
import com.uber.h3core.PolygonToCellsFlags;
import com.uber.h3core.util.LatLng;

/**
 * H3 cell bins and contiguous bin ranges.
 *
 * <p> A bin is the {@link Lexicoders#LONG} encoding of an H3 cell id; H3 ids never set bit 63, so
 * bins sort in numeric id order. With its resolution field set to R, a cell at resolution r agrees
 * with all of its resolution R descendants on every bit except digits r + 1 through R, which run
 * from 0 to 6 in the descendants (digits past R stay 7). The first descendant has those digits all
 * 0 and the last all 6, and every valid resolution R cell whose id lies between the two is a
 * descendant, so a set of cells compacts to one id range per compact cell. Ids in a range that are
 * not valid cells (a digit of 7, or a pentagon's deleted subsequence) never occur as bins.
 */
public final class H3CellRanges {
  private static final int MAX_RESOLUTION = 15;
  private static final int RESOLUTION_OFFSET = 52;
  private static final long RESOLUTION_MASK = 0xFL << RESOLUTION_OFFSET;
  private static final int BASE_CELL_OFFSET = 45;
  private static final int BITS_PER_DIGIT = 3;
  private static final long DIGIT_MASK = 0x7L;
  private static final long MAX_DIGIT = 6;
  // digit 6 (binary 110) in all 15 digit positions
  private static final long ALL_DIGITS_SIX = 0x1B6D_B6DB_6DB6L;
  // H3 reads a polygon edge spanning more than 180 degrees of longitude as crossing the
  // antimeridian, so wider polygons are covered in slabs no wider than this
  private static final double MAX_POLYFILL_DEGREES = 90;
  // H3's native libraries disagree by platform on whether a pole vertex is exactly +/-90; 1e-9
  // degrees (0.1 mm) is far below the smallest resolution 15 edge, about 4e-6 degrees
  private static final double POLE_TOLERANCE_DEGREES = 1e-9;

  private static final Object H3_MUTEX = new Object();
  private static volatile H3Core h3Core;

  private H3CellRanges() {}

  public static H3Core h3() {
    H3Core h3 = h3Core;
    if (h3 == null) {
      synchronized (H3_MUTEX) {
        h3 = h3Core;
        if (h3 == null) {
          try {
            h3 = H3Core.newInstance();
          } catch (final IOException e) {
            throw new IllegalStateException("Unable to load native H3 libraries", e);
          }
          h3Core = h3;
        }
      }
    }
    return h3;
  }

  public static ByteArray toBin(final long cell) {
    return new ByteArray(Lexicoders.LONG.toByteArray(cell));
  }

  public static long fromBin(final byte[] bin) {
    return Lexicoders.LONG.fromByteArray(bin);
  }

  public static long firstDescendant(final long cell, final int resolution) {
    return withResolution(cell, resolution) & ~descendantDigits(cell, resolution);
  }

  public static long lastDescendant(final long cell, final int resolution) {
    final long digits = descendantDigits(cell, resolution);
    return (withResolution(cell, resolution) & ~digits) | (digits & ALL_DIGITS_SIX);
  }

  public static ByteArrayRange descendantRange(final long cell, final int resolution) {
    return range(firstDescendant(cell, resolution), lastDescendant(cell, resolution));
  }

  public static ByteArrayRange[] toRanges(final Collection<Long> cells, final int resolution) {
    // compactCells rejects some duplicates, and a set has none to remove
    final Collection<Long> distinctCells =
        (cells instanceof Set) ? cells : Longs.asList(sortedDistinct(cells));
    for (final long cell : distinctCells) {
      if (resolutionOf(cell) != resolution) {
        throw new IllegalArgumentException(
            "Cell "
                + Long.toHexString(cell)
                + " is at resolution "
                + resolutionOf(cell)
                + ", expected "
                + resolution);
      }
    }
    if (distinctCells.isEmpty()) {
      return new ByteArrayRange[0];
    }
    final long[][] spans =
        h3().compactCells(distinctCells).stream().map(
            cell -> new long[] {
                firstDescendant(cell, resolution),
                lastDescendant(cell, resolution)}).sorted(
                    Comparator.comparingLong(span -> span[0])).toArray(long[][]::new);
    final List<ByteArrayRange> ranges = new ArrayList<>();
    long start = spans[0][0];
    long end = spans[0][1];
    for (int i = 1; i < spans.length; i++) {
      if (spans[i][0] <= nextCellId(end, resolution)) {
        end = Math.max(end, spans[i][1]);
      } else {
        ranges.add(range(start, end));
        start = spans[i][0];
        end = spans[i][1];
      }
    }
    ranges.add(range(start, end));
    return ranges.toArray(new ByteArrayRange[0]);
  }

  /**
   * Ranges of every cell the geometry touches, reading its lon/lat coordinates as planar. The cover
   * is computed at the target resolution before it is compacted, so its cost grows with the number
   * of cells the geometry covers there.
   */
  public static ByteArrayRange[] geometryToRanges(final Geometry geometry, final int resolution) {
    return toRanges(coveringCells(geometry, resolution), resolution);
  }

  /**
   * The cell's boundary as a lon/lat polygon. A cell crossing the antimeridian is split there into
   * a multipolygon, and a cell containing a pole is closed along the pole so that it spans every
   * longitude. A cell with a vertex at a pole is closed along the pole between the longitudes of
   * that vertex's neighbors.
   */
  public static Geometry cellToGeometry(final long cell) {
    final List<LatLng> boundary = splitPoleVertices(h3().cellToBoundary(cell));
    final Coordinate[] ring = new Coordinate[boundary.size() + 1];
    double lng = boundary.get(0).lng;
    for (int i = 0; i < boundary.size(); i++) {
      final LatLng vertex = boundary.get(i);
      lng = vertex.lng + (360 * Math.rint((lng - vertex.lng) / 360));
      ring[i] = new Coordinate(lng, vertex.lat);
    }
    if (Math.rint((lng - boundary.get(0).lng) / 360) != 0) {
      return polarCellGeometry(boundary);
    }
    ring[boundary.size()] = ring[0].copy();
    final List<Polygon> pieces =
        lonLatSlabs(GeometryUtils.GEOMETRY_FACTORY.createPolygon(ring), 360);
    return pieces.size() == 1 ? pieces.get(0)
        : GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon(pieces.toArray(new Polygon[0]));
  }

  static Set<Long> coveringCells(final Geometry geometry, final int resolution) {
    final H3GeometryHandler h3Handler = new H3GeometryHandler(resolution);
    GeometryUtils.visitGeometry(geometry, h3Handler);
    return h3Handler.ids;
  }

  /**
   * The boundary with each vertex within {@link #POLE_TOLERANCE_DEGREES} of a latitude of +/-90,
   * whose longitude is arbitrary, replaced by two vertices at exactly the pole at the longitudes of
   * its neighbors.
   */
  static List<LatLng> splitPoleVertices(final List<LatLng> boundary) {
    final int size = boundary.size();
    final List<LatLng> vertices = new ArrayList<>(size + 1);
    for (int i = 0; i < size; i++) {
      final LatLng vertex = boundary.get(i);
      if ((90 - Math.abs(vertex.lat)) <= POLE_TOLERANCE_DEGREES) {
        final double poleLat = Math.copySign(90, vertex.lat);
        vertices.add(new LatLng(poleLat, boundary.get(((i + size) - 1) % size).lng));
        vertices.add(new LatLng(poleLat, boundary.get((i + 1) % size).lng));
      } else {
        vertices.add(vertex);
      }
    }
    return vertices;
  }

  /**
   * A boundary that winds around a pole, as the polygon between it and the pole over longitudes
   * -180 to 180.
   */
  private static Polygon polarCellGeometry(final List<LatLng> boundary) {
    final int size = boundary.size();
    // start after the one edge crossing the antimeridian so that longitudes run monotonically
    int start = 0;
    for (int i = 0; i < size; i++) {
      if (Math.abs(boundary.get((i + 1) % size).lng - boundary.get(i).lng) > 180) {
        start = (i + 1) % size;
        break;
      }
    }
    final List<Coordinate> path = new ArrayList<>(size + 5);
    for (int i = 0; i < size; i++) {
      final LatLng vertex = boundary.get((start + i) % size);
      path.add(new Coordinate(vertex.lng, vertex.lat));
    }
    if (path.get(0).x > path.get(size - 1).x) {
      Collections.reverse(path);
    }
    final Coordinate west = path.get(0);
    final Coordinate east = path.get(size - 1);
    final double crossingLat =
        east.y + (((west.y - east.y) * (180 - east.x)) / ((west.x + 360) - east.x));
    final double poleLat = Math.signum(west.y) * 90;
    path.add(0, new Coordinate(-180, crossingLat));
    path.add(new Coordinate(180, crossingLat));
    path.add(new Coordinate(180, poleLat));
    path.add(new Coordinate(-180, poleLat));
    path.add(new Coordinate(-180, crossingLat));
    return GeometryUtils.GEOMETRY_FACTORY.createPolygon(path.toArray(new Coordinate[0]));
  }

  /**
   * The polygon cut into slabs of at most the given width of longitude, each shifted by a multiple
   * of 360 degrees into [-180, 180].
   */
  private static List<Polygon> lonLatSlabs(final Polygon polygon, final double width) {
    final Envelope envelope = polygon.getEnvelopeInternal();
    if (!needsSlabs(envelope, width)) {
      return Collections.singletonList(polygon);
    }
    final List<Polygon> pieces = new ArrayList<>();
    final double firstWest = -180 + (Math.floor((envelope.getMinX() + 180) / width) * width);
    for (double west = firstWest; west < envelope.getMaxX(); west += width) {
      final Geometry slab =
          GeometryUtils.GEOMETRY_FACTORY.toGeometry(
              new Envelope(west, west + width, envelope.getMinY(), envelope.getMaxY()));
      Geometry piece = OverlayNGRobust.overlay(polygon, slab, OverlayNG.INTERSECTION);
      final double shift = -360 * Math.floor((west + 180) / 360);
      if (shift != 0) {
        piece = AffineTransformation.translationInstance(shift, 0).transform(piece);
      }
      for (final Object part : PolygonExtracter.getPolygons(piece)) {
        pieces.add((Polygon) part);
      }
    }
    return pieces;
  }

  private static boolean needsSlabs(final Envelope envelope, final double width) {
    return (envelope.getMinX() < -180)
        || (envelope.getMaxX() > 180)
        || (envelope.getWidth() > width);
  }

  private static long[] sortedDistinct(final Collection<Long> cells) {
    final long[] sorted = Longs.toArray(cells);
    Arrays.sort(sorted);
    int size = 0;
    for (final long cell : sorted) {
      if ((size == 0) || (sorted[size - 1] != cell)) {
        sorted[size++] = cell;
      }
    }
    return Arrays.copyOf(sorted, size);
  }

  private static int resolutionOf(final long cell) {
    return (int) ((cell & RESOLUTION_MASK) >>> RESOLUTION_OFFSET);
  }

  private static long withResolution(final long cell, final int resolution) {
    return (cell & ~RESOLUTION_MASK) | ((long) resolution << RESOLUTION_OFFSET);
  }

  private static int digitOffset(final int digit) {
    return (MAX_RESOLUTION - digit) * BITS_PER_DIGIT;
  }

  /** The bits of the digits a cell's descendants at the given resolution add to it. */
  private static long descendantDigits(final long cell, final int resolution) {
    final int cellResolution = resolutionOf(cell);
    if ((resolution < cellResolution) || (resolution > MAX_RESOLUTION)) {
      throw new IllegalArgumentException(
          "Resolution "
              + resolution
              + " is not between the cell's resolution "
              + cellResolution
              + " and "
              + MAX_RESOLUTION);
    }
    return ((1L << ((resolution - cellResolution) * BITS_PER_DIGIT)) - 1) << digitOffset(
        resolution);
  }

  /**
   * The next id in numeric order whose digits 1 through the resolution are all at most 6, counting
   * those digits in base 7 and carrying into the base cell. No valid cell at the resolution lies
   * strictly between the two, so ranges that meet there can be merged.
   */
  private static long nextCellId(final long cell, final int resolution) {
    long next = cell;
    for (int digit = resolution; digit >= 1; digit--) {
      final int offset = digitOffset(digit);
      if (((next >>> offset) & DIGIT_MASK) < MAX_DIGIT) {
        return next + (1L << offset);
      }
      next &= ~(DIGIT_MASK << offset);
    }
    return next + (1L << BASE_CELL_OFFSET);
  }

  private static ByteArrayRange range(final long first, final long last) {
    return new ByteArrayRange(toBin(first).getBytes(), toBin(last).getBytes());
  }

  private static class H3GeometryHandler implements GeometryHandler {
    private final int resolution;
    private final Set<Long> ids = new HashSet<>();

    // Minimal buffer distance in degrees (~0.1mm at the equator).
    // This is only needed to convert a LineString to a valid polygon for the H3 API,
    // not for geometric coverage purposes.
    private static final double MINIMAL_BUFFER_DEGREES = 1e-9;

    public H3GeometryHandler(final int resolution) {
      super();
      this.resolution = resolution;
    }

    @Override
    public void handlePoint(final Point point) {
      if (!point.isEmpty()) {
        ids.add(h3().latLngToCell(point.getY(), point.getX(), resolution));
      }
    }

    @Override
    public void handleLineString(final LineString lineString) {
      // polygonToCellsExperimental needs a 2D polygon, so the line is buffered by a negligible
      // amount (~0.1mm at the equator) purely to make one. With containment_overlapping every cell
      // the buffered shape touches is returned, which for a buffer this small is exactly the set of
      // cells the line itself passes through.
      final Coordinate[] coords = lineString.getCoordinates();
      if (coords.length == 0) {
        return;
      }
      if (coords.length == 1) {
        handlePoint(lineString.getPointN(0));
        return;
      }
      GeometryUtils.visitGeometry(lineString.buffer(MINIMAL_BUFFER_DEGREES), this);
    }

    @Override
    public void handlePolygon(final Polygon polygon) {
      if (polygon.isEmpty()) {
        return;
      }
      if (needsSlabs(polygon.getEnvelopeInternal(), MAX_POLYFILL_DEGREES) && !polygon.isValid()) {
        // the slab overlay throws on an invalid polygon; a collapsed one keeps its line work, as
        // H3 covers a narrow collapsed polygon along its edges
        final GeometryFixer fixer = new GeometryFixer(polygon);
        fixer.setKeepCollapsed(true);
        GeometryUtils.visitGeometry(fixer.getResult(), this);
        return;
      }
      for (final Polygon slab : lonLatSlabs(polygon, MAX_POLYFILL_DEGREES)) {
        polyfill(slab);
      }
    }

    private void polyfill(final Polygon polygon) {
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
                resolution,
                PolygonToCellsFlags.containment_overlapping);

      } else {
        idsToAdd =
            h3().polygonToCellsExperimental(
                Arrays.stream(polygon.getExteriorRing().getCoordinates()).map(
                    c -> new LatLng(c.getY(), c.getX())).collect(Collectors.toList()),
                null,
                resolution,
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
