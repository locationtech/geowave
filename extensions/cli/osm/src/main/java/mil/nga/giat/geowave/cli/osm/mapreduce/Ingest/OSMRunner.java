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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.Schema;
import mil.nga.giat.geowave.cli.osm.operations.options.OSMIngestCommandArgs;
import mil.nga.giat.geowave.cli.osm.types.generated.Node;
import mil.nga.giat.geowave.cli.osm.types.generated.Relation;
import mil.nga.giat.geowave.cli.osm.types.generated.Way;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;

public class OSMRunner extends
		Configured implements
		Tool
{
	private static final Logger log = LoggerFactory.getLogger(OSMRunner.class);
	private org.apache.avro.Schema avroSchema = null;
	private String inputAvroFile = null;

	private final OSMIngestCommandArgs ingestOptions;
	private final AccumuloRequiredOptions accumuloOptions;

	public static void main(
			String[] args )
			throws Exception {
		OSMIngestCommandArgs argv = new OSMIngestCommandArgs();
		DataStorePluginOptions opts = new DataStorePluginOptions();
		opts.selectPlugin(new AccumuloDataStoreFactory().getType());

		OperationParser parser = new OperationParser();
		parser.addAdditionalObject(argv);
		parser.addAdditionalObject(opts);

		CommandLineOperationParams params = parser.parse(args);
		if (params.getSuccessCode() == 0) {
			OSMRunner runner = new OSMRunner(
					argv,
					opts);
			int res = ToolRunner.run(
					new Configuration(),
					runner,
					args);
			System.exit(res);
		}

		System.out.println(params.getSuccessMessage());
		System.exit(params.getSuccessCode());
	}

	public OSMRunner(
			OSMIngestCommandArgs ingestOptions,
			DataStorePluginOptions inputStoreOptions ) {
		this.ingestOptions = ingestOptions;
		if (!inputStoreOptions.getType().equals(
				new AccumuloDataStoreFactory().getType())) {
			throw new RuntimeException(
					"Expected accumulo data store");
		}
		this.accumuloOptions = (AccumuloRequiredOptions) inputStoreOptions.getFactoryOptions();

	}

	public void configureSchema(
			org.apache.avro.Schema avroSchema ) {
		this.avroSchema = avroSchema;
	}

	private void enableLocalityGroups(
			OSMIngestCommandArgs argv )
			throws AccumuloSecurityException,
			AccumuloException,
			TableNotFoundException {
		BasicAccumuloOperations bao = new BasicAccumuloOperations(
				accumuloOptions.getZookeeper(),
				accumuloOptions.getInstance(),
				accumuloOptions.getUser(),
				accumuloOptions.getPassword(),
				accumuloOptions.getGeowaveNamespace());
		bao.createTable(
				argv.getOsmTableName(),
				true,
				true,
				null);

		bao.addLocalityGroup(
				argv.getOsmTableName(),
				Schema.CF.NODE);
		bao.addLocalityGroup(
				argv.getOsmTableName(),
				Schema.CF.WAY);
		bao.addLocalityGroup(
				argv.getOsmTableName(),
				Schema.CF.RELATION);
	}

	@Override
	public int run(
			String[] args )
			throws Exception {

		Configuration conf = this.getConf();
		conf.set(
				"tableName",
				ingestOptions.getQualifiedTableName());
		conf.set(
				"osmVisibility",
				ingestOptions.getVisibilityOptions().getVisibility());

		// job settings
		Job job = Job.getInstance(
				conf,
				ingestOptions.getJobName());
		job.setJarByClass(OSMRunner.class);

		switch (ingestOptions.getMapperType()) {
			case "NODE": {
				configureSchema(Node.getClassSchema());
				inputAvroFile = ingestOptions.getNodesBasePath();
				job.setMapperClass(OSMNodeMapper.class);
				break;
			}
			case "WAY": {
				configureSchema(Way.getClassSchema());
				inputAvroFile = ingestOptions.getWaysBasePath();
				job.setMapperClass(OSMWayMapper.class);
				break;
			}
			case "RELATION": {
				configureSchema(Relation.getClassSchema());
				inputAvroFile = ingestOptions.getRelationsBasePath();
				job.setMapperClass(OSMRelationMapper.class);
				break;
			}
			default:
				break;
		}
		if (avroSchema == null || inputAvroFile == null) {
			throw new MissingArgumentException(
					"argument for mapper type must be one of: NODE, WAY, or RELATION");
		}

		enableLocalityGroups(ingestOptions);

		// input format
		job.setInputFormatClass(AvroKeyInputFormat.class);
		FileInputFormat.setInputPaths(
				job,
				inputAvroFile);
		AvroJob.setInputKeySchema(
				job,
				avroSchema);

		// mappper

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		AccumuloOutputFormat.setConnectorInfo(
				job,
				accumuloOptions.getUser(),
				new PasswordToken(
						accumuloOptions.getPassword()));
		AccumuloOutputFormat.setCreateTables(
				job,
				true);
		AccumuloOutputFormat.setDefaultTableName(
				job,
				ingestOptions.getQualifiedTableName());
		AccumuloOutputFormat.setZooKeeperInstance(
				job,
				new ClientConfiguration().withInstance(
						accumuloOptions.getInstance()).withZkHosts(
						accumuloOptions.getZookeeper()));

		// reducer
		job.setNumReduceTasks(0);

		return job.waitForCompletion(true) ? 0 : -1;
	}

}
