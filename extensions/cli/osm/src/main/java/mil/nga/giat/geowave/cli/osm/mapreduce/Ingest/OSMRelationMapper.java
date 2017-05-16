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
import mil.nga.giat.geowave.cli.osm.types.generated.Primitive;
import mil.nga.giat.geowave.cli.osm.types.generated.Relation;
import mil.nga.giat.geowave.cli.osm.types.generated.RelationMember;

/**
 *
 */
public class OSMRelationMapper extends
		OSMMapperBase<Relation>
{

	@Override
	public void map(
			AvroKey<Relation> key,
			NullWritable value,
			Context context )
			throws IOException,
			InterruptedException {

		Relation relation = key.datum();
		Primitive p = relation.getCommon();

		Mutation m = new Mutation(
				getIdHash(p.getId()));
		// Mutation m = new Mutation(_longWriter.writeField(p.getId()));
		// Mutation m = new Mutation(p.getId().toString());

		put(
				m,
				Schema.CF.RELATION,
				Schema.CQ.ID,
				p.getId());

		int i = 0;
		for (RelationMember rm : relation.getMembers()) {
			put(
					m,
					Schema.CF.RELATION,
					Schema.CQ.getRelationMember(
							Schema.CQ.REFERENCE_ROLEID_PREFIX,
							i),
					rm.getRole());
			put(
					m,
					Schema.CF.RELATION,
					Schema.CQ.getRelationMember(
							Schema.CQ.REFERENCE_MEMID_PREFIX,
							i),
					rm.getMember());
			put(
					m,
					Schema.CF.RELATION,
					Schema.CQ.getRelationMember(
							Schema.CQ.REFERENCE_TYPE_PREFIX,
							i),
					rm.getMemberType().toString());
			i++;
		}

		if (!Long.valueOf(
				0).equals(
				p.getVersion())) {
			put(
					m,
					Schema.CF.RELATION,
					Schema.CQ.VERSION,
					p.getVersion());
		}

		if (!Long.valueOf(
				0).equals(
				p.getTimestamp())) {
			put(
					m,
					Schema.CF.RELATION,
					Schema.CQ.TIMESTAMP,
					p.getTimestamp());
		}

		if (!Long.valueOf(
				0).equals(
				p.getChangesetId())) {
			put(
					m,
					Schema.CF.RELATION,
					Schema.CQ.CHANGESET,
					p.getChangesetId());
		}

		if (!Long.valueOf(
				0).equals(
				p.getUserId())) {
			put(
					m,
					Schema.CF.RELATION,
					Schema.CQ.USER_ID,
					p.getUserId());
		}

		put(
				m,
				Schema.CF.RELATION,
				Schema.CQ.USER_TEXT,
				p.getUserName());
		put(
				m,
				Schema.CF.RELATION,
				Schema.CQ.OSM_VISIBILITY,
				p.getVisible());

		for (Map.Entry<String, String> kvp : p.getTags().entrySet()) {
			put(
					m,
					Schema.CF.RELATION,
					kvp.getKey().toString().getBytes(
							Constants.CHARSET),
					kvp.getValue().toString());
		}

		context.write(
				_tableName,
				m);

	}
}
