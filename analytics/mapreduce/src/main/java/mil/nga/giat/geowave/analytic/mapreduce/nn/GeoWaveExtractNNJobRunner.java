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
package mil.nga.giat.geowave.analytic.mapreduce.nn;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.PartitionParameters.Partition;

public class GeoWaveExtractNNJobRunner extends
		NNJobRunner
{

	public GeoWaveExtractNNJobRunner() {
		super();
		setInputFormatConfiguration(new GeoWaveInputFormatConfiguration());
		setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration());
		super.setReducerCount(4);
	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
		params.addAll(super.getParameters());
		params.addAll(MapReduceParameters.getParameters());
		return params;
	}

	@Override
	public int run(
			PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				MapReduceJobController.getConfiguration(runTimeProperties),
				runTimeProperties);
	}

}
