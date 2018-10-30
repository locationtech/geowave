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
package org.locationtech.geowave.core.ingest.hdfs.mapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;

/**
 * This class is the map-reduce mapper for ingestion with the mapper only.
 */
public class IngestMapper extends
		Mapper<AvroKey, NullWritable, GeoWaveOutputKey, Object>
{
	private IngestWithMapper ingestWithMapper;
	private String globalVisibility;
	private String[] indexNames;

	@Override
	protected void map(
			final AvroKey key,
			final NullWritable value,
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		try (CloseableIterator<GeoWaveData> data = ingestWithMapper.toGeoWaveData(
				key.datum(),
				indexNames,
				globalVisibility)) {
			while (data.hasNext()) {
				final GeoWaveData d = data.next();
				context.write(
						new GeoWaveOutputKey<>(
								d),
						d.getValue());
			}
		}
	}

	@Override
	protected void setup(
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		try {
			final String ingestWithMapperStr = context.getConfiguration().get(
					AbstractMapReduceIngest.INGEST_PLUGIN_KEY);
			final byte[] ingestWithMapperBytes = ByteArrayUtils.byteArrayFromString(ingestWithMapperStr);
			ingestWithMapper = (IngestWithMapper) PersistenceUtils.fromBinary(ingestWithMapperBytes);
			globalVisibility = context.getConfiguration().get(
					AbstractMapReduceIngest.GLOBAL_VISIBILITY_KEY);
			indexNames = AbstractMapReduceIngest.getIndexNames(context.getConfiguration());
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}
}
