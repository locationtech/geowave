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
package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureProjection;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.ConvexHullMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.StoreParameters;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;

/**
 *

 */
public class ConvexHullJobRunner extends
		GeoWaveAnalyticJobRunner
{

	private int zoomLevel = 1;

	public ConvexHullJobRunner() {
		super.setOutputFormatConfiguration(new GeoWaveOutputFormatConfiguration());
	}

	public void setZoomLevel(
			final int zoomLevel ) {
		this.zoomLevel = zoomLevel;
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {
		job.setMapperClass(ConvexHullMapReduce.ConvexHullMap.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setReducerClass(ConvexHullMapReduce.ConvexHullReducer.class);
		job.setReduceSpeculativeExecution(false);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(Object.class);
	}

	@Override
	public Class<?> getScope() {
		return ConvexHullMapReduce.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		runTimeProperties.storeIfEmpty(
				HullParameters.Hull.PROJECTION_CLASS,
				SimpleFeatureProjection.class);
		runTimeProperties.setConfig(
				new ParameterEnum<?>[] {
					HullParameters.Hull.WRAPPER_FACTORY_CLASS,
					HullParameters.Hull.PROJECTION_CLASS,
					HullParameters.Hull.DATA_TYPE_ID,
					HullParameters.Hull.INDEX_ID
				},
				config,
				getScope());
		setReducerCount(runTimeProperties.getPropertyAsInt(
				HullParameters.Hull.REDUCER_COUNT,
				4));
		CentroidManagerGeoWave.setParameters(
				config,
				getScope(),
				runTimeProperties);
		NestedGroupCentroidAssignment.setParameters(
				config,
				getScope(),
				runTimeProperties);

		final int localZoomLevel = runTimeProperties.getPropertyAsInt(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				zoomLevel);
		// getting group from next level, now that the prior level is complete
		NestedGroupCentroidAssignment.setZoomLevel(
				config,
				getScope(),
				localZoomLevel + 1);

		addDataAdapter(
				config,
				getAdapter(
						runTimeProperties,
						HullParameters.Hull.DATA_TYPE_ID,
						HullParameters.Hull.DATA_NAMESPACE_URI));
		checkIndex(
				runTimeProperties,
				HullParameters.Hull.INDEX_ID,
				new SpatialDimensionalityTypeProvider().createPrimaryIndex(
						new SpatialOptions()).getId().getString());
		// HP Fortify "Command Injection" false positive
		// What Fortify considers "externally-influenced input"
		// comes only from users with OS-level access anyway
		return super.run(
				config,
				runTimeProperties);
	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
		params.addAll(super.getParameters());

		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			StoreParameters.StoreParam.INPUT_STORE,
			StoreParameters.StoreParam.OUTPUT_STORE,
			GlobalParameters.Global.BATCH_ID
		}));

		params.addAll(MapReduceParameters.getParameters());
		params.addAll(NestedGroupCentroidAssignment.getParameters());

		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			HullParameters.Hull.WRAPPER_FACTORY_CLASS,
			HullParameters.Hull.PROJECTION_CLASS,
			HullParameters.Hull.REDUCER_COUNT,
			HullParameters.Hull.DATA_TYPE_ID,
			HullParameters.Hull.DATA_NAMESPACE_URI,
			HullParameters.Hull.INDEX_ID
		}));
		return params;
	}

	@Override
	protected String getJobName() {
		return "Convex Hull";
	}

}
