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
package mil.nga.giat.geowave.adapter.vector.plugin;

import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.Hints;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.opengis.coverage.grid.GridGeometry;

import mil.nga.giat.geowave.adapter.vector.render.DistributedRenderOptions;

/**
 * This class can be used as a GeoTools Render Transform
 * ('geowave:DistributedRender') within an SLD on any layer that uses the
 * GeoWave Data Store. An example SLD is provided
 * (example-slds/DistributedRender.sld).
 *
 */
@DescribeProcess(title = "DistributedRender", description = "This process will enable GeoWave to render WMS requests within the server and then this will be responsible for compositing the result client-side.")
public class DistributedRenderProcess
{
	public static final String PROCESS_NAME = "geowave:DistributedRender";

	public static final Hints.Key OPTIONS = new Hints.Key(
			DistributedRenderOptions.class);

	@DescribeResult(name = "result", description = "This is just a pass-through, the key is to provide enough information within invertQuery to perform a map to screen transform")
	public SimpleFeatureCollection execute(
			@DescribeParameter(name = "data", description = "Feature collection containing the rendered image")
			final SimpleFeatureCollection features )
			throws ProcessException {
		// this is a pass through, only used so that legend rendering works
		// appropriately

		// InternalDistributedRenderProcess is what actually can be used as a
		// render transformation to perform distributed rendering, within WMS
		// map request callbacks this transformation will be replaced with
		// InternalDistributedRenderProcess

		// therefore all other calls outside of WMS map requests, such as
		// requesting the legend will behave as expected

		return features;
	}

	public Query invertQuery(
			final Query targetQuery,
			final GridGeometry targetGridGeometry )
			throws ProcessException {
		return targetQuery;
	}
}
