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

import java.util.Arrays;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.cli.osm.operations.options.OSMIngestCommandArgs;
import org.locationtech.geowave.cli.osm.osmfeature.types.features.FeatureDefinitionSet;
import org.locationtech.geowave.core.cli.parser.CommandLineOperationParams;
import org.locationtech.geowave.core.cli.parser.OperationParser;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.AbstractMapReduceIngest;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.datastore.accumulo.AccumuloStoreFactoryFamily;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloRequiredOptions;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.opengis.feature.simple.SimpleFeature;

public class OSMConversionRunner extends
		Configured implements
		Tool
{

	private final OSMIngestCommandArgs ingestOptions;
	private final DataStorePluginOptions inputStoreOptions;

	public static void main(
			final String[] args )
			throws Exception {

		final OSMIngestCommandArgs ingestArgs = new OSMIngestCommandArgs();
		final DataStorePluginOptions opts = new DataStorePluginOptions();
		opts.selectPlugin(new AccumuloStoreFactoryFamily().getType());

		final OperationParser parser = new OperationParser();
		parser.addAdditionalObject(ingestArgs);
		parser.addAdditionalObject(opts);

		final CommandLineOperationParams params = parser.parse(args);
		if (params.getSuccessCode() == 0) {
			final OSMConversionRunner runner = new OSMConversionRunner(
					ingestArgs,
					opts);
			final int res = ToolRunner.run(
					new Configuration(),
					runner,
					args);
			System.exit(res);
		}

		System.out.println(params.getSuccessMessage());
		System.exit(params.getSuccessCode());
	}

	public OSMConversionRunner(
			final OSMIngestCommandArgs ingestOptions,
			final DataStorePluginOptions inputStoreOptions ) {

		this.ingestOptions = ingestOptions;
		if (!inputStoreOptions.getType().equals(
				new AccumuloStoreFactoryFamily().getType())) {
			throw new RuntimeException(
					"Expected accumulo data store");
		}
		this.inputStoreOptions = inputStoreOptions;
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {

		final Configuration conf = getConf();
		final AccumuloRequiredOptions accumuloOptions = (AccumuloRequiredOptions) inputStoreOptions.getFactoryOptions();

		// job settings

		final Job job = Job.getInstance(
				conf,
				ingestOptions.getJobName() + "NodeConversion");
		job.setJarByClass(OSMConversionRunner.class);

		job.getConfiguration().set(
				"osm_mapping",
				ingestOptions.getMappingContents());
		job.getConfiguration().set(
				"arguments",
				ingestOptions.serializeToString());

		if (ingestOptions.getVisibilityOptions().getVisibility() != null) {
			job.getConfiguration().set(
					AbstractMapReduceIngest.GLOBAL_VISIBILITY_KEY,
					ingestOptions.getVisibilityOptions().getVisibility());
		}

		// input format

		AbstractInputFormat.setConnectorInfo(
				job,
				accumuloOptions.getUser(),
				new PasswordToken(
						accumuloOptions.getPassword()));
		InputFormatBase.setInputTableName(
				job,
				ingestOptions.getQualifiedTableName());
		AbstractInputFormat.setZooKeeperInstance(
				job,
				new ClientConfiguration().withInstance(
						accumuloOptions.getInstance()).withZkHosts(
						accumuloOptions.getZookeeper()));
		AbstractInputFormat.setScanAuthorizations(
				job,
				new Authorizations(
						ingestOptions.getVisibilityOptions().getVisibility()));

		final IteratorSetting is = new IteratorSetting(
				50,
				"WholeRow",
				WholeRowIterator.class);
		InputFormatBase.addIterator(
				job,
				is);
		job.setInputFormatClass(AccumuloInputFormat.class);
		final Range r = new Range();
		// final ArrayList<Pair<Text, Text>> columns = new ArrayList<>();
		InputFormatBase.setRanges(
				job,
				Arrays.asList(r));

		// output format
		GeoWaveOutputFormat.setStoreOptions(
				job.getConfiguration(),
				inputStoreOptions);
		final AccumuloOptions options = new AccumuloOptions();
		final AdapterStore as = new AdapterStoreImpl(
				new AccumuloOperations(
						accumuloOptions.getZookeeper(),
						accumuloOptions.getInstance(),
						accumuloOptions.getUser(),
						accumuloOptions.getPassword(),
						accumuloOptions.getGeoWaveNamespace(),
						options),
				options);
		for (final FeatureDataAdapter fda : FeatureDefinitionSet.featureAdapters.values()) {
			as.addAdapter(fda);
			GeoWaveOutputFormat.addDataAdapter(
					job.getConfiguration(),
					fda);
		}

		final Index primaryIndex = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
		GeoWaveOutputFormat.addIndex(
				job.getConfiguration(),
				primaryIndex);
		job.getConfiguration().set(
				AbstractMapReduceIngest.INDEX_NAMES_KEY,
				primaryIndex.getName());

		job.setOutputFormatClass(GeoWaveOutputFormat.class);
		job.setMapOutputKeyClass(GeoWaveOutputKey.class);
		job.setMapOutputValueClass(SimpleFeature.class);

		// mappper

		job.setMapperClass(OSMConversionMapper.class);

		// reducer
		job.setNumReduceTasks(0);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}
