/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.binning;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.GeometryUtils.GeometryHandler;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import com.github.davidmoten.geo.Coverage;
import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import com.google.common.collect.HashMultimap;

class GeohashBinningHelper implements SpatialBinningHelper {
  public GeohashBinningHelper() {
    super();
  }

  @Override
  public ByteArrayConstraints getGeometryConstraints(final Geometry geometry, final int precision) {
    final GeohashGeometryHandler geometryHandler = new GeohashGeometryHandler(precision);
    GeometryUtils.visitGeometry(geometry, geometryHandler);
    // we try to replace all common prefixes with a prefix scan instead of using every individual
    // hash on the query
    // this can really help with query performance
    if (removePrefixes(geometryHandler.hashes)) {
      return new ExplicitConstraints(
          geometryHandler.hashes.stream().map(str -> StringUtils.stringToBinary(str)).map(
              bytes -> new ByteArrayRange(bytes, bytes)).toArray(ByteArrayRange[]::new));
    }
    return new ExplicitConstraints(
        geometryHandler.hashes.stream().map(ByteArray::new).toArray(ByteArray[]::new));
  }

  private static boolean removePrefixes(final Set<String> allHashes) {
    if (allHashes.isEmpty() || allHashes.iterator().next().isEmpty()) {
      return false;
    }
    final HashMultimap<String, String> prefixMap = HashMultimap.create();
    allHashes.forEach(s -> prefixMap.put(s.substring(0, s.length() - 1), s));
    // if there are 32 entries of the same substring that means its prefix is fully covered and we
    // can remove the 32 and replace with the prefix

    // need to make sure the set is mutable because we will also try to find prefixes in this set
    final Set<String> retVal =
        prefixMap.asMap().entrySet().stream().filter(e -> e.getValue().size() == 32).map(
            Entry::getKey).collect(Collectors.toCollection(HashSet::new));
    if (retVal.isEmpty()) {
      return false;
    }
    retVal.forEach(k -> prefixMap.get(k).forEach(v -> allHashes.remove(v)));
    removePrefixes(retVal);
    allHashes.addAll(retVal);
    return true;
  }

  @Override
  public ByteArray[] getSpatialBins(final Geometry geometry, final int precision) {
    final GeohashGeometryHandler geometryHandler = new GeohashGeometryHandler(precision);
    GeometryUtils.visitGeometry(geometry, geometryHandler);

    return geometryHandler.hashes.stream().map(ByteArray::new).toArray(ByteArray[]::new);
  }

  @Override
  public Geometry getBinGeometry(final ByteArray bin, final int precision) {
    final double halfWidth = GeoHash.widthDegrees(precision) / 2;
    final double halfHeight = GeoHash.heightDegrees(precision) / 2;
    final LatLong ll = GeoHash.decodeHash(bin.getString());
    return GeometryUtils.GEOMETRY_FACTORY.toGeometry(
        new Envelope(
            ll.getLon() - halfWidth,
            ll.getLon() + halfWidth,
            ll.getLat() - halfHeight,
            ll.getLat() + halfHeight));
  }

  @Override
  public String binToString(final byte[] binId) {
    return StringUtils.stringFromBinary(binId);
  }

  private static class GeohashGeometryHandler implements GeometryHandler {
    private final int precision;
    private final Set<String> hashes = new HashSet<>();
    private final double halfHeight;
    private final double halfWidth;

    public GeohashGeometryHandler(final int precision) {
      this.precision = precision;
      halfHeight = GeoHash.heightDegrees(precision) / 2;
      halfWidth = GeoHash.widthDegrees(precision) / 2;
    }

    @Override
    public void handlePoint(final Point point) {
      hashes.add(GeoHash.encodeHash(point.getY(), point.getX(), precision));
    }

    @Override
    public void handleLineString(final LineString lineString) {
      final double minx = lineString.getEnvelopeInternal().getMinX();
      final double maxx = lineString.getEnvelopeInternal().getMaxX();
      final double miny = lineString.getEnvelopeInternal().getMinY();
      final double maxy = lineString.getEnvelopeInternal().getMaxY();
      final Coverage coverage = GeoHash.coverBoundingBox(maxy, minx, miny, maxx, precision);
      hashes.addAll(coverage.getHashes().stream().filter(geohash -> {
        final LatLong ll = GeoHash.decodeHash(geohash);
        return lineString.intersects(
            GeometryUtils.GEOMETRY_FACTORY.toGeometry(
                new Envelope(
                    ll.getLon() - halfWidth,
                    ll.getLon() + halfWidth,
                    ll.getLat() - halfHeight,
                    ll.getLat() + halfHeight)));
      }).collect(Collectors.toList()));
    }

    @Override
    public void handlePolygon(final Polygon polygon) {
      final double minx = polygon.getEnvelopeInternal().getMinX();
      final double maxx = polygon.getEnvelopeInternal().getMaxX();
      final double miny = polygon.getEnvelopeInternal().getMinY();
      final double maxy = polygon.getEnvelopeInternal().getMaxY();
      final Coverage coverage = GeoHash.coverBoundingBox(maxy, minx, miny, maxx, precision);
      // this probably should be equalsTopo for completeness but considering this is a shortcut for
      // performance anyways, we use equalsExact which should be faster
      if (polygon.equalsExact(polygon.getEnvelope())) {
        hashes.addAll(coverage.getHashes());
      } else {
        hashes.addAll(coverage.getHashes().stream().filter(geohash -> {
          final LatLong ll = GeoHash.decodeHash(geohash);
          return polygon.intersects(
              GeometryUtils.GEOMETRY_FACTORY.toGeometry(
                  new Envelope(
                      ll.getLon() - halfWidth,
                      ll.getLon() + halfWidth,
                      ll.getLat() - halfHeight,
                      ll.getLat() + halfHeight)));
        }).collect(Collectors.toList()));
      }
    }
  }

}
