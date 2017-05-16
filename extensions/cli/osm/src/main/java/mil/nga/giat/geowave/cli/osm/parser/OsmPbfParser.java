/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.cli.osm.parser;

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
import org.openstreetmap.osmosis.osmbinary.BinaryParser;
import org.openstreetmap.osmosis.osmbinary.Osmformat;
import org.openstreetmap.osmosis.osmbinary.file.BlockInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.cli.osm.types.generated.MemberType;
import mil.nga.giat.geowave.cli.osm.types.generated.Node;
import mil.nga.giat.geowave.cli.osm.types.generated.Primitive;
import mil.nga.giat.geowave.cli.osm.types.generated.Relation;
import mil.nga.giat.geowave.cli.osm.types.generated.RelationMember;
import mil.nga.giat.geowave.cli.osm.types.generated.Way;

public class OsmPbfParser
{

	private static Logger LOGGER = LoggerFactory.getLogger(OsmPbfParser.class);

	public Configuration stageData(
			OsmPbfParserOptions args )
			throws IOException {
		final OsmPbfParserOptions arg = args;
		final Configuration conf = new Configuration();
		conf.set(
				"fs.default.name",
				args.getNameNode());
		conf.set(
				"fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

		FileSystem fs = FileSystem.get(conf);
		Path basePath = new Path(
				arg.getHdfsBasePath());

		if (!fs.exists(basePath)) {
			if (!fs.mkdirs(basePath)) {
				throw new IOException(
						"Unable to create staging directory: " + arg.getNameNode() + arg.getHdfsBasePath());
			}
		}
		Path nodesPath = new Path(
				arg.getNodesBasePath());
		Path waysPath = new Path(
				arg.getWaysBasePath());
		Path relationsPath = new Path(
				arg.getRelationsBasePath());

		final DataFileWriter nodeWriter = new DataFileWriter(
				new GenericDatumWriter());
		final DataFileWriter wayWriter = new DataFileWriter(
				new GenericDatumWriter());
		final DataFileWriter relationWriter = new DataFileWriter(
				new GenericDatumWriter());
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

			nodeWriter.create(
					Node.getClassSchema(),
					nodeOut);
			wayWriter.create(
					Way.getClassSchema(),
					wayOut);
			relationWriter.create(
					Relation.getClassSchema(),
					relationOut);

			parser.setupWriter(
					nodeWriter,
					wayWriter,
					relationWriter);

			Files.walkFileTree(
					Paths.get(args.getIngestDirectory()),
					new SimpleFileVisitor<java.nio.file.Path>() {
						@Override
						// I couldn't figure out how to get rid of the findbugs
						// issue.
						@SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
						public FileVisitResult visitFile(
								java.nio.file.Path file,
								BasicFileAttributes attrs )
								throws IOException {
							if (file.getFileName().toString().endsWith(
									arg.getExtension())) {
								loadFileToHdfs(
										file,
										parser);
							}
							return FileVisitResult.CONTINUE;
						}
					});
		}
		catch (IOException ex) {
			LOGGER.error(
					"Unable to crrate the FSDataOutputStream",
					ex);
		}
		finally {
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
			java.nio.file.Path file,
			OsmAvroBinaryParser parser ) {

		InputStream is = null;
		try {
			is = new FileInputStream(
					file.toFile());
			new BlockInputStream(
					is,
					parser).process();
		}
		catch (FileNotFoundException e) {
			LOGGER.error(
					"Unable to load file: " + file.toString(),
					e);
		}
		catch (IOException e1) {
			LOGGER.error(
					"Unable to process file: " + file.toString(),
					e1);
		}
		finally {
			IOUtils.closeQuietly(is);
		}

	}

	private static class OsmAvroBinaryParser extends
			BinaryParser
	{
		private static Logger LOGGER = LoggerFactory.getLogger(OsmAvroBinaryParser.class);

		private DataFileWriter nodeWriter = null;
		private DataFileWriter wayWriter = null;
		private DataFileWriter relationWriter = null;

		public void setupWriter(
				DataFileWriter nodeWriter,
				DataFileWriter wayWriter,
				DataFileWriter relationWriter ) {
			this.nodeWriter = nodeWriter;
			this.wayWriter = wayWriter;
			this.relationWriter = relationWriter;
		}

		@Override
		protected void parseRelations(
				List<Osmformat.Relation> rels ) {
			for (Osmformat.Relation r : rels) {
				Relation r2 = new Relation();
				Primitive p = getPrimitive(r.getInfo());
				p.setId(r.getId());
				p.setTags(getTags(
						r.getKeysList(),
						r.getValsList()));
				r2.setCommon(p);

				List<RelationMember> members = new ArrayList<>(
						r.getRolesSidCount());

				for (int i = 0; i < r.getRolesSidCount(); i++) {
					RelationMember rm = new RelationMember();
					rm.setMember(r.getMemids(i));
					rm.setRole(getStringById(r.getRolesSid(i)));
					switch (r.getTypes(
							i).toString()) {
						case "NODE": {
							rm.setMemberType(MemberType.NODE);
							break;
						}
						case "WAY": {
							rm.setMemberType(MemberType.WAY);
							break;
						}
						case "RELATION": {
							rm.setMemberType(MemberType.RELATION);
							break;
						}
						default:
							break;
					}

				}
				r2.setMembers(members);
				try {
					relationWriter.append(r2);
				}
				catch (IOException e) {
					LOGGER.error(
							"Unable to write relation",
							e);
				}
			}
		}

		@Override
		protected void parseDense(
				Osmformat.DenseNodes nodes ) {
			long lastId = 0;
			long lastLat = 0;
			long lastLon = 0;
			long lastTimestamp = 0;
			long lastChangeset = 0;
			int lastUid = 0;
			int lastSid = 0;

			int tagLocation = 0;

			for (int i = 0; i < nodes.getIdCount(); i++) {

				Node n = new Node();
				Primitive p = new Primitive();

				lastId += nodes.getId(i);
				lastLat += nodes.getLat(i);
				lastLon += nodes.getLon(i);

				p.setId(lastId);
				n.setLatitude(parseLat(lastLat));
				n.setLongitude(parseLon(lastLon));

				// Weird spec - keys and values are mashed sequentially, and end
				// of data for a particular node is denoted by a value of 0
				if (nodes.getKeysValsCount() > 0) {
					Map<String, String> tags = new HashMap<>(
							nodes.getKeysValsCount());
					while (nodes.getKeysVals(tagLocation) > 0) {
						String k = getStringById(nodes.getKeysVals(tagLocation));
						tagLocation++;
						String v = getStringById(nodes.getKeysVals(tagLocation));
						tagLocation++;
						tags.put(
								k,
								v);
					}
					p.setTags(tags);
				}

				if (nodes.hasDenseinfo()) {
					Osmformat.DenseInfo di = nodes.getDenseinfo();
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
				}
				catch (IOException e) {
					LOGGER.error(
							"Unable to write dense node",
							e);
				}

			}
		}

		@Override
		protected void parseNodes(
				List<Osmformat.Node> nodes ) {
			for (Osmformat.Node n : nodes) {
				Node n2 = new Node();
				Primitive p = getPrimitive(n.getInfo());
				p.setId(n.getId());
				p.setTags(getTags(
						n.getKeysList(),
						n.getValsList()));
				n2.setCommon(p);
				n2.setLatitude(parseLat(n.getLat()));
				n2.setLongitude(parseLon(n.getLon()));
				try {
					nodeWriter.append(n2);
				}
				catch (IOException e) {
					LOGGER.error(
							"Unable to write node",
							e);
				}
			}
		}

		@Override
		protected void parseWays(
				List<Osmformat.Way> ways ) {
			for (Osmformat.Way w : ways) {
				Way w2 = new Way();
				Primitive p = getPrimitive(w.getInfo());
				p.setId(w.getId());
				p.setTags(getTags(
						w.getKeysList(),
						w.getValsList()));
				w2.setCommon(p);

				long lastRef = 0;
				List<Long> nodes = new ArrayList<>(
						w.getRefsCount());
				for (Long ref : w.getRefsList()) {
					lastRef += ref;
					nodes.add(lastRef);
				}
				w2.setNodes(nodes);

				try {
					wayWriter.append(w2);
				}
				catch (IOException e) {
					LOGGER.error(
							"Unable to write way",
							e);
				}
			}
		}

		@Override
		protected void parse(
				Osmformat.HeaderBlock header ) {

		}

		public void complete() {
			System.out.println("Complete!");
		}

		private Map<String, String> getTags(
				List<Integer> k,
				List<Integer> v ) {
			Map<String, String> tags = new HashMap<String, String>(
					k.size());
			for (int i = 0; i < k.size(); i++) {
				tags.put(
						getStringById(k.get(i)),
						getStringById(v.get(i)));
			}
			return tags;
		}

		private Primitive getPrimitive(
				Osmformat.Info info ) {
			Primitive p = new Primitive();
			p.setVersion((long) info.getVersion());
			p.setTimestamp(info.getTimestamp());
			p.setUserId((long) info.getUid());
			try {
				p.setUserName(getStringById(info.getUid()));
			}
			catch (Exception ex) {
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
