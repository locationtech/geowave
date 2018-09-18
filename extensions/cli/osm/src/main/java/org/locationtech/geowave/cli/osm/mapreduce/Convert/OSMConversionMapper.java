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
package org.locationtech.geowave.cli.osm.mapreduce.Convert;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.mapreduce.Mapper;
import org.locationtech.geowave.cli.osm.mapreduce.Convert.OsmProvider.OsmProvider;
import org.locationtech.geowave.cli.osm.operations.options.OSMIngestCommandArgs;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.AbstractMapReduceIngest;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloRequiredOptions;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.opengis.feature.simple.SimpleFeature;

public class OSMConversionMapper extends
		Mapper<Key, Value, GeoWaveOutputKey, Object>
{

	private String indexName = null;
	private String globalVisibility = "";
	private final SimpleFeatureGenerator sfg = new SimpleFeatureGenerator();
	private OsmProvider osmProvider = null;

	@Override
	protected void map(
			final Key key,
			final Value value,
			final Context context )
			throws IOException,
			InterruptedException {
		final List<SimpleFeature> sf = sfg.mapOSMtoSimpleFeature(
				WholeRowIterator.decodeRow(
						key,
						value),
				osmProvider);
		if ((sf != null) && (sf.size() > 0)) {
			for (final SimpleFeature feat : sf) {
				final String name = feat.getType().getTypeName();
				context.write(
						new GeoWaveOutputKey(
								name,
								indexName),
						feat);
			}
		}
	}

	@Override
	protected void cleanup(
			final Context context )
			throws IOException,
			InterruptedException {
		osmProvider.close();

		super.cleanup(context);
	}

	@Override
	protected void setup(
			final Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		try {
			globalVisibility = context.getConfiguration().get(
					AbstractMapReduceIngest.GLOBAL_VISIBILITY_KEY);
			final String primaryIndexIdStr = context.getConfiguration().get(
					AbstractMapReduceIngest.INDEX_NAMES_KEY);
			if (primaryIndexIdStr != null) {
				indexName = primaryIndexIdStr;
			}
			final OSMIngestCommandArgs args = new OSMIngestCommandArgs();
			args.deserializeFromString(context.getConfiguration().get(
					"arguments"));

			final DataStorePluginOptions storeOptions = GeoWaveOutputFormat.getStoreOptions(context);

			osmProvider = new OsmProvider(
					args,
					(AccumuloRequiredOptions) storeOptions.getFactoryOptions());
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}
}
