/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.osm.mapreduce.Convert.OsmProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.locationtech.geowave.cli.osm.accumulo.osmschema.ColumnFamily;
import org.locationtech.geowave.cli.osm.accumulo.osmschema.ColumnQualifier;
import org.locationtech.geowave.cli.osm.accumulo.osmschema.Schema;
import org.locationtech.geowave.cli.osm.mapreduce.Convert.SimpleFeatureGenerator;
import org.locationtech.geowave.cli.osm.operations.options.OSMIngestCommandArgs;
import org.locationtech.geowave.cli.osm.osmfeature.types.features.FeatureDefinition;
import org.locationtech.geowave.cli.osm.types.TypeUtils;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.datastore.accumulo.config.AccumuloRequiredOptions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Joiner;

public class OsmProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(OsmProvider.class);
  private Connector conn = null;
  private BatchScanner bs = null;
  private final FieldReader<Long> longReader = FieldUtils.getDefaultReaderForClass(Long.class);
  private final FieldReader<Double> doubleReader =
      FieldUtils.getDefaultReaderForClass(Double.class);

  public OsmProvider(final OSMIngestCommandArgs args, final AccumuloRequiredOptions store)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    conn =
        new ZooKeeperInstance(store.getInstance(), store.getZookeeper()).getConnector(
            store.getUser(),
            new PasswordToken(store.getPassword()));
    bs =
        conn.createBatchScanner(
            args.getQualifiedTableName(),
            new Authorizations(args.getVisibilityOptions().getGlobalVisibility()),
            1);
  }

  public Geometry processRelation(
      final SimpleFeatureGenerator.OSMUnion osmunion,
      final FeatureDefinition fd) {

    // multipolygon type
    if ((osmunion.relationSets != null)
        && (osmunion.relationSets.size() > 0)
        && (osmunion.tags != null)
        && "multipolygon".equals(osmunion.tags.get("type"))) {

      final Map<String, List<LinearRing>> rings = waysFromAccumulo(osmunion.relationSets, osmunion);

      if (rings == null) {
        return null;
      }

      final List<LinearRing> outer = rings.get("outer");
      final List<LinearRing> inner = rings.get("inner");

      if (outer.size() == 0) {
        LOGGER.error(
            "Polygons must have at least one outer ring; error with relation: " + osmunion.Id);
        return null;
      }

      final List<Polygon> polygons = new ArrayList<>();

      for (final LinearRing lr : outer) {
        final List<LinearRing> tempInner = new ArrayList<>();
        for (final LinearRing i : inner) {
          if (lr.contains(i)) {
            tempInner.add(i);
          }
        }
        polygons.add(
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                lr,
                tempInner.toArray(new LinearRing[tempInner.size()])));
      }

      if (polygons.size() == 0) {
        LOGGER.error("No polygons built for relation: " + osmunion.Id);
        return null;
      }

      if (polygons.size() == 1) {
        return polygons.get(0);
      }

      return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon(
          polygons.toArray(new Polygon[polygons.size()]));
    }
    LOGGER.info("Unsupported relation type for relation: " + osmunion.Id);
    // todo admin boundaries, routes, etc:
    // http://wiki.openstreetmap.org/wiki/Types_of_relation
    return null;
  }

  public Geometry processWay(
      final SimpleFeatureGenerator.OSMUnion osmunion,
      final FeatureDefinition fd) {

    if ((osmunion.Nodes == null) || (osmunion.Nodes.size() == 0)) {
      return null;
    }

    final Map<Long, Coordinate> coords = nodesFromAccumulo(osmunion.Nodes);
    final Coordinate[] orderedCoords = new Coordinate[osmunion.Nodes.size()];

    final List<String> missingNodes = new ArrayList<>();

    int i = 0;
    for (final long l : osmunion.Nodes) {
      // String hash = new String(Schema.getIdHash(l));

      orderedCoords[i] = (coords.get(l));
      if (orderedCoords[i] == null) {
        // System.out.println("missing point for way: " + osmunion.Id);
        missingNodes.add(String.valueOf(l));
      }
      i++;
    }

    // if we are missing portions geometry is invalid; log it and return
    // null
    if (missingNodes.size() != 0) {
      LOGGER.error(
          "Some of the nodes for Way: "
              + osmunion.Id
              + " were not present.  Nodes missing were: ("
              + Joiner.on(",").join(missingNodes)
              + ")");
      return null;
    }

    if ((osmunion.Nodes.size() > 2)
        && (osmunion.Nodes.get(0) == osmunion.Nodes.get(osmunion.Nodes.size() - 1))) {
      // closed way
      switch (fd.type) {
        case Geometry: { // best guess on type = polygon (closed way)
          return GeometryUtils.GEOMETRY_FACTORY.createPolygon(orderedCoords);
        }
        case Polygon: {
          return GeometryUtils.GEOMETRY_FACTORY.createPolygon(orderedCoords);
        }
        case LineString: {
          return GeometryUtils.GEOMETRY_FACTORY.createLineString(orderedCoords);
        }
        case Point: {
          return GeometryUtils.GEOMETRY_FACTORY.createPolygon(orderedCoords).getCentroid();
        }
      }
    } else {
      // open way
      switch (fd.type) {
        case Geometry: { // best guess on type
          final String area = osmunion.tags.get("area");
          if ((area != null) && "yes".equals(area)) {
            // close the geometry - it's supposto be an area
            final Coordinate[] closedCords = Arrays.copyOf(orderedCoords, orderedCoords.length + 1);
            closedCords[closedCords.length - 1] = closedCords[0];
            return GeometryUtils.GEOMETRY_FACTORY.createPolygon(closedCords);
          } else {
            return GeometryUtils.GEOMETRY_FACTORY.createLineString(orderedCoords);
          }
        }
        case Polygon: {
          if (orderedCoords.length < 3) {
            LOGGER.warn(
                "Geometry type Polygon requested for unclosed way, but not enough points (4) would be present after closing.  Relation id: "
                    + osmunion.Id);
            return null;
          }
          // close the geometry since it's unclosed, but coereced to a
          // polygon
          final Coordinate[] closedCords = Arrays.copyOf(orderedCoords, orderedCoords.length + 1);
          closedCords[closedCords.length - 1] = closedCords[0];
          return GeometryUtils.GEOMETRY_FACTORY.createPolygon(closedCords);
        }
        case LineString: {
          return GeometryUtils.GEOMETRY_FACTORY.createLineString(orderedCoords);
        }
        case Point: {
          return GeometryUtils.GEOMETRY_FACTORY.createLineString(orderedCoords).getCentroid();
        }
      }
    }

    // default case, shouldn't be hit;
    LOGGER.error(
        "Way: "
            + osmunion.Id
            + " did not parse correctly; geometry generation was not caught and fell through");
    return null;
  }

  public void close() {
    if (bs != null) {
      bs.close();
    }
  }

  private Map<String, List<LinearRing>> waysFromAccumulo(
      final Map<Integer, SimpleFeatureGenerator.RelationSet> relations,
      final SimpleFeatureGenerator.OSMUnion osmunion) {

    final Map<String, List<LinearRing>> rings = new HashMap<>();
    rings.put("inner", new ArrayList<LinearRing>());
    rings.put("outer", new ArrayList<LinearRing>());

    final List<Long> outerWays = new ArrayList<>();
    final List<Long> innerWays = new ArrayList<>();

    for (final Map.Entry<Integer, SimpleFeatureGenerator.RelationSet> kvp : relations.entrySet()) {
      switch (kvp.getValue().memType) {
        case RELATION: {
          LOGGER.warn("Super-relations not currently supported");
          return null;
        }
        case WAY: {
          if ("outer".equals(kvp.getValue().roleId)) {
            outerWays.add(kvp.getValue().memId);
          } else if ("inner".equals(kvp.getValue().roleId)) {
            innerWays.add(kvp.getValue().memId);
          }
          break;
        }
        case NODE: {
          LOGGER.warn("Nodes as direct members of relationships not currently supported");
          return null;
        }
      }
    }

    final List<Range> ranges = new ArrayList<>(outerWays.size() + innerWays.size());
    if (ranges.size() == 0) {
      LOGGER.warn("No multipolygon relations found for relation: " + osmunion.Id);
      return null;
    }

    for (final Long l : outerWays) {
      final byte[] row = Schema.getIdHash(l);
      ranges.add(new Range(new Text(row)));
    }
    for (final Long l : innerWays) {
      final byte[] row = Schema.getIdHash(l);
      ranges.add(new Range(new Text(row)));
    }

    bs.setRanges(ranges);
    bs.clearColumns();
    bs.fetchColumn(new Text(ColumnFamily.WAY), new Text(ColumnQualifier.ID));
    bs.fetchColumn(new Text(ColumnFamily.WAY), new Text(ColumnQualifier.REFERENCES));

    final Map<Long, List<Long>> vals = new HashMap<>();

    long id = -1;
    List<Long> tvals = null;
    ByteSequence lastkey = null;

    for (final Map.Entry<Key, Value> row : bs) {
      if (lastkey == null) {
        lastkey = row.getKey().getRowData();
      }

      if (Schema.arraysEqual(
          row.getKey().getColumnQualifierData(),
          StringUtils.stringToBinary(ColumnQualifier.ID))) {
        id = longReader.readField(row.getValue().get());
      } else if (Schema.arraysEqual(
          row.getKey().getColumnQualifierData(),
          StringUtils.stringToBinary(ColumnQualifier.REFERENCES))) {
        try {
          tvals = TypeUtils.deserializeLongArray(row.getValue().get(), null).getIds();
        } catch (final IOException e) {
          LOGGER.error("Error deserializing member array for way: ", e);
        }
      }

      if ((id != -1) && (tvals != null)) {
        vals.put(id, tvals);
        tvals = null;
        id = -1;
        lastkey = null;
      } else if (!lastkey.equals(row.getKey().getRowData())) {
        tvals = null;
        id = -1;
        lastkey = null;
      }
    }

    for (final Map.Entry<Long, List<Long>> kvp : vals.entrySet()) {
      final Map<Long, Coordinate> ring = nodesFromAccumulo(kvp.getValue());
      Coordinate[] sortedCoords = new Coordinate[ring.size()];
      final List<String> missingIds = new ArrayList<>();
      int i = 0;
      for (final long l : kvp.getValue()) {
        sortedCoords[i] = ring.get(l);
        if (sortedCoords[i] == null) {

          missingIds.add(String.valueOf(l));
        }
        i++;
      }
      if (missingIds.size() != 0) {
        LOGGER.error(
            "Error building ring relation for relation: "
                + osmunion.Id
                + " missing values were: ("
                + Joiner.on(",").join(missingIds)
                + ")");
        return null;
      }

      if (sortedCoords[0] != sortedCoords[sortedCoords.length - 1]) {
        // ring not closed, should be by definition -f ix
        final Coordinate[] closedCords = Arrays.copyOf(sortedCoords, sortedCoords.length + 1);
        closedCords[sortedCoords.length + 1] = closedCords[0];
        sortedCoords = closedCords;
      }

      if (sortedCoords.length < 4) {
        LOGGER.error(
            "Not enough coordinates for way: " + kvp.getKey() + " for relation: " + osmunion.Id);
        return null;
      }

      final LinearRing lr = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(sortedCoords);

      if (innerWays.contains(kvp.getKey())) {
        rings.get("inner").add(lr);
      } else if (outerWays.contains(kvp.getKey())) {
        rings.get("outer").add(lr);
      } else {
        LOGGER.error("Relation not found in inner or outer for way: " + kvp.getKey());
        return null;
      }
    }
    return rings;
  }

  private Map<Long, Coordinate> nodesFromAccumulo(final List<Long> vals) {

    List<Range> ranges = new ArrayList<>(vals.size());
    for (final Long l : vals) {
      final byte[] row = Schema.getIdHash(l);
      ranges.add(new Range(new Text(row)));
      // ranges.add(new Range(l.toString()));
    }
    ranges = Range.mergeOverlapping(ranges);

    bs.setRanges(ranges);
    bs.clearColumns();
    // bs.fetchColumnFamily(new Text(Schema.CF.NODE));
    bs.fetchColumn(new Text(ColumnFamily.NODE), new Text(ColumnQualifier.LONGITUDE));
    bs.fetchColumn(new Text(ColumnFamily.NODE), new Text(ColumnQualifier.LATITUDE));
    bs.fetchColumn(new Text(ColumnFamily.NODE), new Text(ColumnQualifier.ID));

    final Map<Long, Coordinate> coords = new HashMap<>();

    long id = -1L;
    Coordinate crd = new Coordinate(-200, -200);
    ByteSequence lastkey = null;

    for (final Map.Entry<Key, Value> row : bs) {
      if (lastkey == null) {
        lastkey = row.getKey().getRowData();
      }

      if (Schema.arraysEqual(
          row.getKey().getColumnQualifierData(),
          StringUtils.stringToBinary(ColumnQualifier.LONGITUDE))) {
        crd.x = doubleReader.readField(row.getValue().get());
      } else if (Schema.arraysEqual(
          row.getKey().getColumnQualifierData(),
          StringUtils.stringToBinary(ColumnQualifier.LATITUDE))) {
        crd.y = doubleReader.readField(row.getValue().get());
      } else if (Schema.arraysEqual(
          row.getKey().getColumnQualifierData(),
          StringUtils.stringToBinary(ColumnQualifier.ID))) {
        id = longReader.readField(row.getValue().get());
      }

      if ((id != -1L) && (crd.x >= -180) && (crd.y >= -180)) {
        coords.put(id, crd);
        id = -1L;
        crd = new Coordinate(-200, -200);
        lastkey = null;
      } else if (!lastkey.equals(row.getKey().getRowData())) {
        id = -1L;
        crd = new Coordinate(-200, -200);
        lastkey = null;
      }
    }
    return coords;
  }
}
