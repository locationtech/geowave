/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.osm.mapreduce.Ingest;

import java.io.IOException;
import java.util.Map;
import org.apache.accumulo.core.data.Mutation;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.locationtech.geowave.cli.osm.accumulo.osmschema.ColumnFamily;
import org.locationtech.geowave.cli.osm.accumulo.osmschema.ColumnQualifier;
import org.locationtech.geowave.cli.osm.types.avro.AvroLongArray;
import org.locationtech.geowave.cli.osm.types.avro.AvroPrimitive;
import org.locationtech.geowave.cli.osm.types.avro.AvroWay;

/** */
public class OSMWayMapper extends OSMMapperBase<AvroWay> {

  @Override
  public void map(final AvroKey<AvroWay> key, final NullWritable value, final Context context)
      throws IOException, InterruptedException {

    final AvroWay way = key.datum();
    final AvroPrimitive p = way.getCommon();

    final Mutation m = new Mutation(getIdHash(p.getId()));
    // Mutation m = new Mutation(_longWriter.writeField(p.getId()));
    // Mutation m = new Mutation(p.getId().toString());

    put(m, ColumnFamily.WAY, ColumnQualifier.ID, p.getId());

    final AvroLongArray lr = new AvroLongArray();
    lr.setIds(way.getNodes());

    put(m, ColumnFamily.WAY, ColumnQualifier.REFERENCES, lr);

    if (!Long.valueOf(0).equals(p.getVersion())) {
      put(m, ColumnFamily.WAY, ColumnQualifier.VERSION, p.getVersion());
    }

    if (!Long.valueOf(0).equals(p.getTimestamp())) {
      put(m, ColumnFamily.WAY, ColumnQualifier.TIMESTAMP, p.getTimestamp());
    }

    if (!Long.valueOf(0).equals(p.getChangesetId())) {
      put(m, ColumnFamily.WAY, ColumnQualifier.CHANGESET, p.getChangesetId());
    }

    if (!Long.valueOf(0).equals(p.getUserId())) {
      put(m, ColumnFamily.WAY, ColumnQualifier.USER_ID, p.getUserId());
    }

    put(m, ColumnFamily.WAY, ColumnQualifier.USER_TEXT, p.getUserName());
    put(m, ColumnFamily.WAY, ColumnQualifier.OSM_VISIBILITY, p.getVisible());

    for (final Map.Entry<String, String> kvp : p.getTags().entrySet()) {
      put(m, ColumnFamily.WAY, kvp.getKey(), kvp.getValue());
    }

    context.write(_tableName, m);
  }
}
