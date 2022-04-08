/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.binning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.GeometryUtils.GeometryHandler;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import com.google.common.collect.Streams;
import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2PolygonBuilder;
import com.google.common.geometry.S2Polyline;
import com.google.common.geometry.S2Region;
import com.google.common.geometry.S2RegionCoverer;

class S2BinningHelper implements SpatialBinningHelper {
  public S2BinningHelper() {
    super();
  }

  @Override
  public ByteArrayConstraints getGeometryConstraints(final Geometry geom, final int precision) {
    final S2RegionCoverer coverer = new S2RegionCoverer();
    coverer.setMaxCells(100);
    // no sense decomposing further than the max precision the stats are binned at
    coverer.setMaxLevel(precision);
    final S2CellUnion s2CellUnion = cellCoverage(geom, coverer);
    return new ExplicitConstraints(
        Streams.stream(s2CellUnion.iterator()).map(
            c -> new ByteArrayRange(
                Lexicoders.LONG.toByteArray(c.rangeMin().id()),
                Lexicoders.LONG.toByteArray(c.rangeMax().id()))).toArray(ByteArrayRange[]::new));
  }

  @Override
  public String binToString(final byte[] binId) {
    final Long id = Lexicoders.LONG.fromByteArray(binId);
    return new S2CellId(id).toToken();
  }

  private static S2CellUnion cellCoverage(final Geometry geom, final S2RegionCoverer coverer) {
    // this probably should be equalsTopo for completeness but considering this is a shortcut for
    // performance anyways, we use equalsExact which should be faster
    if (geom.equalsExact(geom.getEnvelope())) {
      final double minx = geom.getEnvelopeInternal().getMinX();
      final double maxx = geom.getEnvelopeInternal().getMaxX();
      final double miny = geom.getEnvelopeInternal().getMinY();
      final double maxy = geom.getEnvelopeInternal().getMaxY();
      final S2Region s2Region =
          new S2LatLngRect(S2LatLng.fromDegrees(miny, minx), S2LatLng.fromDegrees(maxy, maxx));

      return coverer.getCovering(s2Region);
    } else {
      final S2GeometryHandler geometryHandler = new S2GeometryHandler(coverer);
      GeometryUtils.visitGeometry(geom, geometryHandler);
      return geometryHandler.cellUnion;
    }
  }

  @Override
  public ByteArray[] getSpatialBins(final Geometry geometry, final int precision) {
    if (geometry instanceof Point) {
      final Point centroid = geometry.getCentroid();
      return new ByteArray[] {
          new ByteArray(
              Lexicoders.LONG.toByteArray(
                  S2CellId.fromLatLng(
                      S2LatLng.fromDegrees(centroid.getY(), centroid.getX())).parent(
                          precision).id()))};
    } else {
      return getSpatialBinsComplexGeometry(geometry, precision);
    }
  }

  @Override
  public Geometry getBinGeometry(final ByteArray bin, final int precision) {
    final Long id = Lexicoders.LONG.fromByteArray(bin.getBytes());

    final List<Coordinate> coords =
        IntStream.range(0, 4).mapToObj(i -> new S2Cell(new S2CellId(id)).getVertex(i)).map(
            S2LatLng::new).map(ll -> new Coordinate(ll.lngDegrees(), ll.latDegrees())).collect(
                Collectors.toList());
    // we need to close it so the first one needs to repeat at the end
    coords.add(coords.get(0));
    return GeometryUtils.GEOMETRY_FACTORY.createPolygon(coords.toArray(new Coordinate[5]));
  }

  private static ByteArray[] getSpatialBinsComplexGeometry(
      final Geometry geometry,
      final int precision) {
    final S2RegionCoverer coverer = new S2RegionCoverer();
    // for now lets assume 10000 should cover any polygon at the desired precision
    coverer.setMaxCells(10000);
    coverer.setMinLevel(precision);
    coverer.setMaxLevel(precision);
    final S2CellUnion cellUnion = cellCoverage(geometry, coverer);
    final ArrayList<S2CellId> cellIds = new ArrayList<>();
    // because cell unions are automatically normalized (children fully covering a parent get
    // collapsed into a parent) we need to get the covering at the desired precision so we must
    // denormalize (this is where memory concerns could come in for abnormally large polygons)
    cellUnion.denormalize(precision, 1, cellIds);
    return cellIds.stream().map(S2CellId::id).map(Lexicoders.LONG::toByteArray).map(
        ByteArray::new).toArray(ByteArray[]::new);
  }

  @Override
  public int getBinByteLength(final int precision) {
    return Long.BYTES;
  }

  private static class S2GeometryHandler implements GeometryHandler {

    private S2CellUnion cellUnion;
    private final S2RegionCoverer coverer;

    public S2GeometryHandler(final S2RegionCoverer coverer) {
      super();
      cellUnion = new S2CellUnion();
      this.coverer = coverer;
    }


    @Override
    public void handlePoint(final Point point) {
      final S2CellUnion newUnion = new S2CellUnion();
      final ArrayList<S2CellId> cellIds = cellUnion.cellIds();
      cellIds.add(S2CellId.fromLatLng(S2LatLng.fromDegrees(point.getY(), point.getX())));
      newUnion.initFromCellIds(cellIds);
      cellUnion = newUnion;
    }

    @Override
    public void handleLineString(final LineString lineString) {
      final S2CellUnion newUnion = new S2CellUnion();
      newUnion.getUnion(
          coverer.getCovering(
              new S2Polyline(
                  Arrays.stream(lineString.getCoordinates()).map(
                      c -> S2LatLng.fromDegrees(c.getY(), c.getX()).toPoint()).collect(
                          Collectors.toList()))),
          cellUnion);
      cellUnion = newUnion;
    }

    @Override
    public void handlePolygon(final Polygon polygon) {
      // order matters for S2, exterior ring must be counter clockwise and interior must be
      // clockwise (respecting the right-hand rule)
      polygon.normalize();
      final S2PolygonBuilder bldr = new S2PolygonBuilder();
      final int numInteriorRings = polygon.getNumInteriorRing();
      if (numInteriorRings > 0) {
        for (int i = 0; i < numInteriorRings; i++) {
          final LineString ls = polygon.getInteriorRingN(i);

          bldr.addLoop(
              new S2Loop(
                  Arrays.stream(ls.getCoordinates()).map(
                      c -> S2LatLng.fromDegrees(c.getY(), c.getX()).toPoint()).collect(
                          Collectors.toList())));
        }
      }

      bldr.addLoop(
          new S2Loop(
              Arrays.stream(polygon.getExteriorRing().getCoordinates()).map(
                  c -> S2LatLng.fromDegrees(c.getY(), c.getX()).toPoint()).collect(
                      Collectors.toList())));
      final S2CellUnion newUnion = new S2CellUnion();
      newUnion.getUnion(coverer.getCovering(bldr.assemblePolygon()), cellUnion);
      cellUnion = newUnion;
    }
  }
}
