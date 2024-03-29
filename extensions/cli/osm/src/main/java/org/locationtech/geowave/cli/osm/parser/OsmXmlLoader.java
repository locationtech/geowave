/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.osm.parser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.openstreetmap.osmosis.xml.v0_6.XmlReader;

public class OsmXmlLoader implements Sink {

  /** All nodes in this OSM XML */
  protected final List<Node> nodes = new LinkedList<>();
  /** All ways in this OSM XML */
  protected final List<Way> ways = new LinkedList<>();
  /** All relations in this OSM XML */
  protected final List<Relation> relations = new LinkedList<>();

  /** Mapping: Node ID -> Node object. Useful to resolve Way -> Node dependencies */
  protected Map<Long, Node> nodeById = new HashMap<>();

  // getters

  public List<Node> getNodes() {
    return nodes;
  }

  public List<Way> getWays() {
    return ways;
  }

  public List<Relation> getRelations() {
    return relations;
  }

  public Node getNodeById(final long id) {
    return nodeById.get(id);
  }

  public Node getNodeById(final WayNode wayNode) {
    return nodeById.get(wayNode.getNodeId());
  }

  public List<Node> getNodesById(final Way way) throws IOException {
    final List<Node> wayNodes = new ArrayList<>(way.getWayNodes().size());
    for (final WayNode wn : way.getWayNodes()) {
      final Node n = getNodeById(wn);
      if (n == null) {
        throw new IOException(
            String.format(
                "Error while parsing OSM XML: Node %s in Way %s (length: %s) is not declared in the document!",
                wn.getNodeId(),
                way.getId(),
                way.getWayNodes().size()));
      }
      wayNodes.add(n);
    }
    return wayNodes;
  }

  // Sink implementation

  @Override
  public void process(final EntityContainer entityContainer) {
    final Entity entity = entityContainer.getEntity();
    if (entity instanceof Node) {
      final Node node = (Node) entity;
      nodes.add(node);
      nodeById.put(node.getId(), node);
    } else if (entity instanceof Way) {
      final Way way = (Way) entity;
      ways.add(way);
    } else if (entity instanceof Relation) {
      final Relation rel = (Relation) entity;
      relations.add(rel);
    }
  }

  protected void processRelations() {
    /*
     * // TODO add relation support or find a lib that handles this for (Relation rel : relations) {
     * List<RelationMember> members = rel.getMembers(); members.get( 0).getMemberId(); }
     */
  }

  @Override
  public void initialize(final Map<String, Object> metaData) {
    /* unused */
  }

  @Override
  public void complete() {
    /* unused */
  }

  @Override
  public void release() {
    /* unused */
  }

  // Instantiation

  public static OsmXmlLoader readOsmXml(final File osmxml) {

    // Defines the interface for tasks consuming OSM data types.
    final OsmXmlLoader sink = new OsmXmlLoader();

    // compression (if any)
    CompressionMethod compression = CompressionMethod.None;
    if (osmxml.getName().endsWith(".gz")) {
      compression = CompressionMethod.GZip;
    } else if (osmxml.getName().endsWith(".bz2")) {
      compression = CompressionMethod.BZip2;
    }

    // read source file (into sink)
    final XmlReader reader = new XmlReader(osmxml, false, compression);
    reader.setSink(sink);
    reader.run(); // just run, no threading

    return sink;
  }

  // print helpers

  public static void print(final Node node) {
    System.out.format(
        "%s: %10s (%-10s, %-10s), version %2s by %s%n",
        "Node",
        node.getId(),
        node.getLatitude(),
        node.getLongitude(),
        String.valueOf(node.getVersion()),
        node.getUser().getName());
    printTags(node.getTags());
  }

  public static void print(final Way way) {
    System.out.format(
        "%s: %10s, version %2s by %s with %s waypoints%n",
        "Way",
        way.getId(),
        way.getVersion(),
        way.getUser().getName(),
        way.getWayNodes().size());
    printTags(way.getTags());
  }

  public static void printTags(final Collection<Tag> tags) {
    if (tags.size() > 0) {
      System.out.format("\tTags: %s%n", formatTags(tags));
    }
  }

  public static String formatTags(final Collection<Tag> tags) {
    final StringBuilder sb = new StringBuilder(tags.size() * 20);
    for (final Tag tag : tags) {
      sb.append(", ");
      sb.append(tag.getKey());
      sb.append('=');
      sb.append(tag.getValue());
    }
    if (sb.length() > 2) {
      sb.delete(0, 2);
    }
    return sb.toString();
  }
}
