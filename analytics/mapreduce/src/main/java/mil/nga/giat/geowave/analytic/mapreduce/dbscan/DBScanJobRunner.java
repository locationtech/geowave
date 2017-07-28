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
package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.util.Arrays;
import java.util.Collection;

import mil.nga.giat.geowave.analytic.AdapterWithObjectWritable;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.Projection;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureProjection;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce.PartitionDataWritable;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters.Clustering;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters.Global;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters.Hull;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.PartitionParameters.Partition;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.geotools.feature.type.BasicFeatureTypes;

/**
 * Run a single DBScan job producing micro clusters over a set of neighbors
 */
public class DBScanJobRunner extends
		NNJobRunner
{

	private static final String[] CodecsRank = new String[] {
		"BZip2",
		// "Gzip",
		"Lz4",
		"Snappy",
		"Lzo",
	};

	private boolean firstIteration = true;
	private long memInMB = 4096;

	@Override
	public void configure(
			final Job job )
			throws Exception {
		super.configure(job);
		job.setMapperClass(NNMapReduce.NNMapper.class);
		job.setReducerClass(DBScanMapReduce.DBScanMapHullReducer.class);
		job.setMapOutputKeyClass(PartitionDataWritable.class);
		job.setMapOutputValueClass(AdapterWithObjectWritable.class);
		job.setOutputKeyClass(GeoWaveInputKey.class);
		job.setOutputValueClass(ObjectWritable.class);
		job.setSpeculativeExecution(false);
		final Configuration conf = job.getConfiguration();
		conf.set(
				"mapreduce.map.java.opts",
				"-Xmx" + memInMB + "m");
		conf.set(
				"mapreduce.reduce.java.opts",
				"-Xmx" + memInMB + "m");
		conf.setLong(
				"mapred.task.timeout",
				2000000);
		conf.setInt(
				"mapreduce.task.io.sort.mb",
				250);
		job.getConfiguration().setBoolean(
				"mapreduce.reduce.speculative",
				false);

		Class<? extends CompressionCodec> bestCodecClass = org.apache.hadoop.io.compress.DefaultCodec.class;
		int rank = 0;
		for (final Class<? extends CompressionCodec> codecClass : CompressionCodecFactory.getCodecClasses(conf)) {
			int r = 1;
			for (final String codecs : CodecsRank) {
				if (codecClass.getName().contains(
						codecs)) {
					break;
				}
				r++;
			}
			if ((rank < r) && (r <= CodecsRank.length)) {
				try {
					final CompressionCodec codec = codecClass.newInstance();
					if (Configurable.class.isAssignableFrom(codecClass)) {
						((Configurable) codec).setConf(conf);
					}
					// throws an exception if not configurable in this context
					CodecPool.getCompressor(codec);
					bestCodecClass = codecClass;
					rank = r;
				}
				catch (final Throwable ex) {
					// occurs when codec is not installed.
					LOGGER.warn(
							"Not configuable in this context",
							ex);
				}
			}
		}
		LOGGER.warn("Compression with " + bestCodecClass.toString());

		conf.setClass(
				"mapreduce.map.output.compress.codec",
				bestCodecClass,
				CompressionCodec.class);
		conf.setBoolean(
				"mapreduce.map.output.compress",
				true);
		conf.setBooleanIfUnset(
				"first.iteration",
				firstIteration);
	}

	public void setMemoryInMB(
			final long memInMB ) {
		this.memInMB = memInMB;
	}

	protected void setFirstIteration(
			final boolean firstIteration ) {
		this.firstIteration = firstIteration;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		runTimeProperties.storeIfEmpty(
				HullParameters.Hull.DATA_TYPE_ID,
				"concave_hull");
		final String adapterID = runTimeProperties.getPropertyAsString(
				HullParameters.Hull.DATA_TYPE_ID,
				"concave_hull");
		final String namespaceURI = runTimeProperties.storeIfEmpty(
				HullParameters.Hull.DATA_NAMESPACE_URI,
				BasicFeatureTypes.DEFAULT_NAMESPACE).toString();

		JobContextAdapterStore.addDataAdapter(
				config,
				AnalyticFeature.createGeometryFeatureAdapter(
						adapterID,
						new String[0],
						namespaceURI,
						ClusteringUtils.CLUSTERING_CRS));

		final Projection<?> projectionFunction = runTimeProperties.getClassInstance(
				HullParameters.Hull.PROJECTION_CLASS,
				Projection.class,
				SimpleFeatureProjection.class);

		projectionFunction.setup(
				runTimeProperties,
				getScope(),
				config);

		runTimeProperties.setConfig(
				new ParameterEnum[] {
					HullParameters.Hull.PROJECTION_CLASS,
					GlobalParameters.Global.BATCH_ID,
					HullParameters.Hull.ZOOM_LEVEL,
					HullParameters.Hull.ITERATION,
					HullParameters.Hull.DATA_TYPE_ID,
					HullParameters.Hull.DATA_NAMESPACE_URI,
					ClusteringParameters.Clustering.MINIMUM_SIZE,
					Partition.GEOMETRIC_DISTANCE_UNIT,
					Partition.DISTANCE_THRESHOLDS,
					Partition.MAX_MEMBER_SELECTION
				},
				config,
				getScope());

		// HP Fortify "Command Injection" false positive
		// What Fortify considers "externally-influenced input"
		// comes only from users with OS-level access anyway
		return super.run(
				config,
				runTimeProperties);

	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Collection<ParameterEnum<?>> params = super.getParameters();
		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			Partition.PARTITIONER_CLASS,
			Partition.MAX_DISTANCE,
			Partition.MAX_MEMBER_SELECTION,
			Global.BATCH_ID,
			Hull.DATA_TYPE_ID,
			Hull.PROJECTION_CLASS,
			Clustering.MINIMUM_SIZE,
			Partition.GEOMETRIC_DISTANCE_UNIT,
			Partition.DISTANCE_THRESHOLDS
		}));
		return params;
	}

}
