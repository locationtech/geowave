/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.cli.osm.mapreduce.Ingest;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Mutation;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.locationtech.geowave.cli.osm.accumulo.osmschema.ColumnFamily;
import org.locationtech.geowave.cli.osm.accumulo.osmschema.ColumnQualifier;
import org.locationtech.geowave.cli.osm.types.generated.Primitive;
import org.locationtech.geowave.cli.osm.types.generated.Relation;
import org.locationtech.geowave.cli.osm.types.generated.RelationMember;

/**
 *
 */
public class OSMRelationMapper extends
		OSMMapperBase<Relation>
{

	@Override
	public void map(
			final AvroKey<Relation> key,
			final NullWritable value,
			final Context context )
			throws IOException,
			InterruptedException {

		final Relation relation = key.datum();
		final Primitive p = relation.getCommon();

		final Mutation m = new Mutation(
				getIdHash(p.getId()));
		// Mutation m = new Mutation(_longWriter.writeField(p.getId()));
		// Mutation m = new Mutation(p.getId().toString());

		put(
				m,
				ColumnFamily.RELATION,
				ColumnQualifier.ID,
				p.getId());

		int i = 0;
		for (final RelationMember rm : relation.getMembers()) {
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.getRelationMember(
							ColumnQualifier.REFERENCE_ROLEID_PREFIX,
							i),
					rm.getRole());
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.getRelationMember(
							ColumnQualifier.REFERENCE_MEMID_PREFIX,
							i),
					rm.getMember());
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.getRelationMember(
							ColumnQualifier.REFERENCE_TYPE_PREFIX,
							i),
					rm.getMemberType().toString());
			i++;
		}

		if (!Long.valueOf(
				0).equals(
				p.getVersion())) {
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.VERSION,
					p.getVersion());
		}

		if (!Long.valueOf(
				0).equals(
				p.getTimestamp())) {
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.TIMESTAMP,
					p.getTimestamp());
		}

		if (!Long.valueOf(
				0).equals(
				p.getChangesetId())) {
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.CHANGESET,
					p.getChangesetId());
		}

		if (!Long.valueOf(
				0).equals(
				p.getUserId())) {
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.USER_ID,
					p.getUserId());
		}

		put(
				m,
				ColumnFamily.RELATION,
				ColumnQualifier.USER_TEXT,
				p.getUserName());
		put(
				m,
				ColumnFamily.RELATION,
				ColumnQualifier.OSM_VISIBILITY,
				p.getVisible());

		for (final Map.Entry<String, String> kvp : p.getTags().entrySet()) {
			put(
					m,
					ColumnFamily.RELATION,
					kvp.getKey().toString(),
					kvp.getValue().toString());
		}

		context.write(
				_tableName,
				m);

	}
}
