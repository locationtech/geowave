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
package mil.nga.giat.geowave.adapter.vector.export;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.vector.AvroFeatureUtils;
import mil.nga.giat.geowave.adapter.vector.avro.AttributeValues;
import mil.nga.giat.geowave.adapter.vector.avro.AvroSimpleFeatureCollection;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class VectorExportMapper extends
		Mapper<GeoWaveInputKey, SimpleFeature, AvroKey<AvroSimpleFeatureCollection>, NullWritable>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Logger.class);
	private int batchSize;
	private final Map<ByteArrayId, AvroSFCWriter> adapterIdToAvroWriterMap = new HashMap<ByteArrayId, AvroSFCWriter>();
	private final NullWritable outVal = NullWritable.get();
	private final AvroKey<AvroSimpleFeatureCollection> outKey = new AvroKey<AvroSimpleFeatureCollection>();

	@Override
	protected void map(
			final GeoWaveInputKey key,
			final SimpleFeature value,
			final Mapper<GeoWaveInputKey, SimpleFeature, AvroKey<AvroSimpleFeatureCollection>, NullWritable>.Context context )
			throws IOException,
			InterruptedException {
		AvroSFCWriter avroWriter = adapterIdToAvroWriterMap.get(key.getAdapterId());
		if (avroWriter == null) {
			avroWriter = new AvroSFCWriter(
					value.getFeatureType(),
					batchSize);
			adapterIdToAvroWriterMap.put(
					key.getAdapterId(),
					avroWriter);
		}
		final AvroSimpleFeatureCollection retVal = avroWriter.write(value);
		if (retVal != null) {
			outKey.datum(retVal);
			context.write(
					outKey,
					outVal);
		}
	}

	@Override
	protected void setup(
			final Mapper<GeoWaveInputKey, SimpleFeature, AvroKey<AvroSimpleFeatureCollection>, NullWritable>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		batchSize = context.getConfiguration().getInt(
				VectorMRExportJobRunner.BATCH_SIZE_KEY,
				VectorExportOptions.DEFAULT_BATCH_SIZE);
	}

	@Override
	protected void cleanup(
			final Mapper<GeoWaveInputKey, SimpleFeature, AvroKey<AvroSimpleFeatureCollection>, NullWritable>.Context context )
			throws IOException,
			InterruptedException {
		super.cleanup(context);
		writeRemainingAvroBatches(context);
	}

	private void writeRemainingAvroBatches(
			final Mapper<GeoWaveInputKey, SimpleFeature, AvroKey<AvroSimpleFeatureCollection>, NullWritable>.Context context )
			throws IOException,
			InterruptedException {
		for (final AvroSFCWriter writer : adapterIdToAvroWriterMap.values()) {
			if (writer.avList.size() > 0) {
				writer.simpleFeatureCollection.setSimpleFeatureCollection(writer.avList);
				outKey.datum(writer.simpleFeatureCollection);
				context.write(
						outKey,
						outVal);
			}
		}
	}

	private static class AvroSFCWriter
	{
		private final int batchSize;
		private final SimpleFeatureType sft;

		private AvroSimpleFeatureCollection simpleFeatureCollection = null;
		private List<AttributeValues> avList = null;

		private AvroSFCWriter(
				final SimpleFeatureType sft,
				final int batchSize ) {
			this.sft = sft;
			this.batchSize = batchSize;
		}

		private AvroSimpleFeatureCollection write(
				final SimpleFeature feature ) {
			AvroSimpleFeatureCollection retVal = null;
			if (simpleFeatureCollection == null) {
				newFeatureCollection();
			}
			else if (avList.size() >= batchSize) {
				simpleFeatureCollection.setSimpleFeatureCollection(avList);
				retVal = simpleFeatureCollection;
				newFeatureCollection();
			}
			final AttributeValues av = AvroFeatureUtils.buildAttributeValue(
					feature,
					sft);
			avList.add(av);
			return retVal;
		}

		// this isn't intended to be thread safe
		private void newFeatureCollection() {
			simpleFeatureCollection = new AvroSimpleFeatureCollection();
			try {
				simpleFeatureCollection.setFeatureType(AvroFeatureUtils.buildFeatureDefinition(
						null,
						sft,
						null,
						""));
			}
			catch (final IOException e) {
				// this should never actually happen, deault classification is
				// passed in
				LOGGER.warn(
						"Unable to find classification",
						e);
			}
			avList = new ArrayList<AttributeValues>(
					batchSize);
		}
	}

}
