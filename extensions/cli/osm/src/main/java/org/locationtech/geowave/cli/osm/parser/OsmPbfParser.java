/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.osm.parser;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.locationtech.geowave.cli.osm.types.avro.AvroMemberType;
import org.locationtech.geowave.cli.osm.types.avro.AvroNode;
import org.locationtech.geowave.cli.osm.types.avro.AvroPrimitive;
import org.locationtech.geowave.cli.osm.types.avro.AvroRelation;
import org.locationtech.geowave.cli.osm.types.avro.AvroRelationMember;
import org.locationtech.geowave.cli.osm.types.avro.AvroWay;
import org.openstreetmap.osmosis.osmbinary.BinaryParser;
import org.openstreetmap.osmosis.osmbinary.Osmformat;
import org.openstreetmap.osmosis.osmbinary.file.BlockInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class OsmPbfParser {

  private static Logger LOGGER = LoggerFactory.getLogger(OsmPbfParser.class);

  public Configuration stageData(final OsmPbfParserOptions args) throws IOException {
    final OsmPbfParserOptions arg = args;
    final Configuration conf = new Configuration();
    conf.set("fs.default.name", args.getNameNode());
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

    final FileSystem fs = FileSystem.get(conf);
    final Path basePath = new Path(arg.getHdfsBasePath());

    if (!fs.exists(basePath)) {
      if (!fs.mkdirs(basePath)) {
        throw new IOException(
            "Unable to create staging directory: " + arg.getNameNode() + arg.getHdfsBasePath());
      }
    }
    final Path nodesPath = new Path(arg.getNodesBasePath());
    final Path waysPath = new Path(arg.getWaysBasePath());
    final Path relationsPath = new Path(arg.getRelationsBasePath());

    final DataFileWriter nodeWriter = new DataFileWriter(new GenericDatumWriter());
    final DataFileWriter wayWriter = new DataFileWriter(new GenericDatumWriter());
    final DataFileWriter relationWriter = new DataFileWriter(new GenericDatumWriter());
    nodeWriter.setCodec(CodecFactory.snappyCodec());
    wayWriter.setCodec(CodecFactory.snappyCodec());
    relationWriter.setCodec(CodecFactory.snappyCodec());
    FSDataOutputStream nodeOut = null;
    FSDataOutputStream wayOut = null;
    FSDataOutputStream relationOut = null;

    final OsmAvroBinaryParser parser = new OsmAvroBinaryParser();
    try {

      nodeOut = fs.create(nodesPath);
      wayOut = fs.create(waysPath);
      relationOut = fs.create(relationsPath);

      nodeWriter.create(AvroNode.getClassSchema(), nodeOut);
      wayWriter.create(AvroWay.getClassSchema(), wayOut);
      relationWriter.create(AvroRelation.getClassSchema(), relationOut);

      parser.setupWriter(nodeWriter, wayWriter, relationWriter);

      Files.walkFileTree(
          Paths.get(args.getIngestDirectory()),
          new SimpleFileVisitor<java.nio.file.Path>() {
            @Override
            // I couldn't figure out how to get rid of the findbugs
            // issue.
            @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
            public FileVisitResult visitFile(
                final java.nio.file.Path file,
                final BasicFileAttributes attrs) throws IOException {
              if (file.getFileName().toString().endsWith(arg.getExtension())) {
                loadFileToHdfs(file, parser);
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (final IOException ex) {
      LOGGER.error("Unable to crrate the FSDataOutputStream", ex);
    } finally {
      IOUtils.closeQuietly(nodeWriter);
      IOUtils.closeQuietly(wayWriter);
      IOUtils.closeQuietly(relationWriter);
      IOUtils.closeQuietly(nodeOut);
      IOUtils.closeQuietly(wayOut);
      IOUtils.closeQuietly(relationOut);
      fs.close();
    }

    return conf;
  }

  private static void loadFileToHdfs(
      final java.nio.file.Path file,
      final OsmAvroBinaryParser parser) {

    InputStream is = null;
    try {
      is = new FileInputStream(file.toFile());
      new BlockInputStream(is, parser).process();
    } catch (final FileNotFoundException e) {
      LOGGER.error("Unable to load file: " + file.toString(), e);
    } catch (final IOException e1) {
      LOGGER.error("Unable to process file: " + file.toString(), e1);
    } finally {
      IOUtils.closeQuietly(is);
    }
  }

  private static class OsmAvroBinaryParser extends BinaryParser {
    private static Logger LOGGER = LoggerFactory.getLogger(OsmAvroBinaryParser.class);

    private DataFileWriter nodeWriter = null;
    private DataFileWriter wayWriter = null;
    private DataFileWriter relationWriter = null;

    public void setupWriter(
        final DataFileWriter nodeWriter,
        final DataFileWriter wayWriter,
        final DataFileWriter relationWriter) {
      this.nodeWriter = nodeWriter;
      this.wayWriter = wayWriter;
      this.relationWriter = relationWriter;
    }

    @Override
    protected void parseRelations(final List<Osmformat.Relation> rels) {
      for (final Osmformat.Relation r : rels) {
        final AvroRelation r2 = new AvroRelation();
        final AvroPrimitive p = getPrimitive(r.getInfo());
        p.setId(r.getId());
        p.setTags(getTags(r.getKeysList(), r.getValsList()));
        r2.setCommon(p);

        final List<AvroRelationMember> members = new ArrayList<>(r.getRolesSidCount());

        for (int i = 0; i < r.getRolesSidCount(); i++) {
          final AvroRelationMember rm = new AvroRelationMember();
          rm.setMember(r.getMemids(i));
          rm.setRole(getStringById(r.getRolesSid(i)));
          switch (r.getTypes(i).toString()) {
            case "NODE": {
              rm.setMemberType(AvroMemberType.NODE);
              break;
            }
            case "WAY": {
              rm.setMemberType(AvroMemberType.WAY);
              break;
            }
            case "RELATION": {
              rm.setMemberType(AvroMemberType.RELATION);
              break;
            }
            default:
              break;
          }
        }
        r2.setMembers(members);
        try {
          relationWriter.append(r2);
        } catch (final IOException e) {
          LOGGER.error("Unable to write relation", e);
        }
      }
    }

    @Override
    protected void parseDense(final Osmformat.DenseNodes nodes) {
      long lastId = 0;
      long lastLat = 0;
      long lastLon = 0;
      long lastTimestamp = 0;
      long lastChangeset = 0;
      int lastUid = 0;
      int lastSid = 0;

      int tagLocation = 0;

      for (int i = 0; i < nodes.getIdCount(); i++) {

        final AvroNode n = new AvroNode();
        final AvroPrimitive p = new AvroPrimitive();

        lastId += nodes.getId(i);
        lastLat += nodes.getLat(i);
        lastLon += nodes.getLon(i);

        p.setId(lastId);
        n.setLatitude(parseLat(lastLat));
        n.setLongitude(parseLon(lastLon));

        // Weird spec - keys and values are mashed sequentially, and end
        // of data for a particular node is denoted by a value of 0
        if (nodes.getKeysValsCount() > 0) {
          final Map<String, String> tags = new HashMap<>(nodes.getKeysValsCount());
          while (nodes.getKeysVals(tagLocation) > 0) {
            final String k = getStringById(nodes.getKeysVals(tagLocation));
            tagLocation++;
            final String v = getStringById(nodes.getKeysVals(tagLocation));
            tagLocation++;
            tags.put(k, v);
          }
          p.setTags(tags);
        }

        if (nodes.hasDenseinfo()) {
          final Osmformat.DenseInfo di = nodes.getDenseinfo();
          lastTimestamp += di.getTimestamp(i);
          lastChangeset += di.getChangeset(i);
          lastUid += di.getUid(i);
          lastSid += di.getUserSid(i);

          p.setTimestamp(lastTimestamp);
          p.setChangesetId(lastChangeset);
          p.setUserId((long) lastUid);
          p.setUserName(getStringById(lastSid));
          if (di.getVisibleCount() > 0) {
            p.setVisible(di.getVisible(i));
          }
        }

        n.setCommon(p);

        try {
          nodeWriter.append(n);
        } catch (final IOException e) {
          LOGGER.error("Unable to write dense node", e);
        }
      }
    }

    @Override
    protected void parseNodes(final List<Osmformat.Node> nodes) {
      for (final Osmformat.Node n : nodes) {
        final AvroNode n2 = new AvroNode();
        final AvroPrimitive p = getPrimitive(n.getInfo());
        p.setId(n.getId());
        p.setTags(getTags(n.getKeysList(), n.getValsList()));
        n2.setCommon(p);
        n2.setLatitude(parseLat(n.getLat()));
        n2.setLongitude(parseLon(n.getLon()));
        try {
          nodeWriter.append(n2);
        } catch (final IOException e) {
          LOGGER.error("Unable to write node", e);
        }
      }
    }

    @Override
    protected void parseWays(final List<Osmformat.Way> ways) {
      for (final Osmformat.Way w : ways) {
        final AvroWay w2 = new AvroWay();
        final AvroPrimitive p = getPrimitive(w.getInfo());
        p.setId(w.getId());
        p.setTags(getTags(w.getKeysList(), w.getValsList()));
        w2.setCommon(p);

        long lastRef = 0;
        final List<Long> nodes = new ArrayList<>(w.getRefsCount());
        for (final Long ref : w.getRefsList()) {
          lastRef += ref;
          nodes.add(lastRef);
        }
        w2.setNodes(nodes);

        try {
          wayWriter.append(w2);
        } catch (final IOException e) {
          LOGGER.error("Unable to write way", e);
        }
      }
    }

    @Override
    protected void parse(final Osmformat.HeaderBlock header) {}

    @Override
    public void complete() {
      System.out.println("Complete!");
    }

    private Map<String, String> getTags(final List<Integer> k, final List<Integer> v) {
      final Map<String, String> tags = new HashMap<>(k.size());
      for (int i = 0; i < k.size(); i++) {
        tags.put(getStringById(k.get(i)), getStringById(v.get(i)));
      }
      return tags;
    }

    private AvroPrimitive getPrimitive(final Osmformat.Info info) {
      final AvroPrimitive p = new AvroPrimitive();
      p.setVersion((long) info.getVersion());
      p.setTimestamp(info.getTimestamp());
      p.setUserId((long) info.getUid());
      try {
        p.setUserName(getStringById(info.getUid()));
      } catch (final Exception ex) {
        LOGGER.warn(
            "Error, input file doesn't contain a valid string table for user id: " + info.getUid(),
            ex);
        p.setUserName(String.valueOf(info.getUid()));
      }
      p.setChangesetId(info.getChangeset());
      p.setVisible(info.getVisible());
      return p;
    }
  }
}
