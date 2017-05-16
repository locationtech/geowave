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
package mil.nga.giat.geowave.cli.osm.mapreduce.Ingest;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Mutation;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.Constants;
import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.Schema;
import mil.nga.giat.geowave.cli.osm.types.generated.LongArray;
import mil.nga.giat.geowave.cli.osm.types.generated.Primitive;
import mil.nga.giat.geowave.cli.osm.types.generated.Way;

/**
 *
 */
public class OSMWayMapper extends
		OSMMapperBase<Way>
{

	@Override
	public void map(
			AvroKey<Way> key,
			NullWritable value,
			Context context )
			throws IOException,
			InterruptedException {

		Way way = key.datum();
		Primitive p = way.getCommon();

		Mutation m = new Mutation(
				getIdHash(p.getId()));
		// Mutation m = new Mutation(_longWriter.writeField(p.getId()));
		// Mutation m = new Mutation(p.getId().toString());

		put(
				m,
				Schema.CF.WAY,
				Schema.CQ.ID,
				p.getId());

		LongArray lr = new LongArray();
		lr.setIds(way.getNodes());

		put(
				m,
				Schema.CF.WAY,
				Schema.CQ.REFERENCES,
				lr);

		if (!Long.valueOf(
				0).equals(
				p.getVersion())) {
			put(
					m,
					Schema.CF.WAY,
					Schema.CQ.VERSION,
					p.getVersion());
		}

		if (!Long.valueOf(
				0).equals(
				p.getTimestamp())) {
			put(
					m,
					Schema.CF.WAY,
					Schema.CQ.TIMESTAMP,
					p.getTimestamp());
		}

		if (!Long.valueOf(
				0).equals(
				p.getChangesetId())) {
			put(
					m,
					Schema.CF.WAY,
					Schema.CQ.CHANGESET,
					p.getChangesetId());
		}

		if (!Long.valueOf(
				0).equals(
				p.getUserId())) {
			put(
					m,
					Schema.CF.WAY,
					Schema.CQ.USER_ID,
					p.getUserId());
		}

		put(
				m,
				Schema.CF.WAY,
				Schema.CQ.USER_TEXT,
				p.getUserName());
		put(
				m,
				Schema.CF.WAY,
				Schema.CQ.OSM_VISIBILITY,
				p.getVisible());

		for (Map.Entry<String, String> kvp : p.getTags().entrySet()) {
			put(
					m,
					Schema.CF.WAY,
					kvp.getKey().toString().getBytes(
							Constants.CHARSET),
					kvp.getValue().toString());
		}

		context.write(
				_tableName,
				m);

	}
}
