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
package mil.nga.giat.geowave.cli.osm.mapreduce.Convert;

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
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.cli.osm.operations.options.OSMIngestCommandArgs;
import mil.nga.giat.geowave.cli.osm.osmfeature.types.features.FeatureDefinitionSet;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.AbstractMapReduceIngest;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

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
		opts.selectPlugin(new AccumuloDataStoreFactory().getType());

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
				new AccumuloDataStoreFactory().getType())) {
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

		final AdapterStore as = new AccumuloAdapterStore(
				new BasicAccumuloOperations(
						accumuloOptions.getZookeeper(),
						accumuloOptions.getInstance(),
						accumuloOptions.getUser(),
						accumuloOptions.getPassword(),
						accumuloOptions.getGeowaveNamespace()));
		for (final FeatureDataAdapter fda : FeatureDefinitionSet.featureAdapters.values()) {
			as.addAdapter(fda);
			GeoWaveOutputFormat.addDataAdapter(
					job.getConfiguration(),
					fda);
		}

		final PrimaryIndex primaryIndex = new SpatialDimensionalityTypeProvider()
				.createPrimaryIndex(new SpatialOptions());
		GeoWaveOutputFormat.addIndex(
				job.getConfiguration(),
				primaryIndex);
		job.getConfiguration().set(
				AbstractMapReduceIngest.PRIMARY_INDEX_IDS_KEY,
				StringUtils.stringFromBinary(primaryIndex.getId().getBytes()));

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
