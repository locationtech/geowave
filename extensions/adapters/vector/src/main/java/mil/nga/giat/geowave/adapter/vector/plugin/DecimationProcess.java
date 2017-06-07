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
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.opengis.coverage.grid.GridGeometry;

/**
 * This class can be used as a GeoTools Render Transform ('nga:Decimation')
 * within an SLD on any layer that uses the GeoWave Data Store. An example SLD
 * is provided (example-slds/DecimatePoints.sld). The pixel-size allows you to
 * skip more than a single pixel. For example, a pixel size of 3 would skip an
 * estimated 3x3 pixel cell in GeoWave's row IDs. Note that rows are only
 * skipped when a feature successfully passes filters.
 * 
 */
@SuppressWarnings("deprecation")
@DescribeProcess(title = "DecimateToPixelResolution", description = "This process will enable GeoWave to decimate WMS rendering down to pixel resolution to not oversample data.  This will efficiently render overlapping geometry that would otherwise be hidden but it assume an opaque style and does not take transparency into account.")
public class DecimationProcess implements
		GSProcess
{
	public static final Hints.Key PIXEL_SIZE = new Hints.Key(
			Double.class);
	public static final Hints.Key OUTPUT_BBOX = new Hints.Key(
			ReferencedEnvelope.class);
	public static final Hints.Key OUTPUT_WIDTH = new Hints.Key(
			Integer.class);
	public static final Hints.Key OUTPUT_HEIGHT = new Hints.Key(
			Integer.class);

	@DescribeResult(name = "result", description = "This is just a pass-through, the key is to provide enough information within invertQuery to perform a map to screen transform")
	public SimpleFeatureCollection execute(
			@DescribeParameter(name = "data", description = "Feature collection containing the data")
			final SimpleFeatureCollection features,
			@DescribeParameter(name = "outputBBOX", description = "Georeferenced bounding box of the output")
			final ReferencedEnvelope argOutputEnv,
			@DescribeParameter(name = "outputWidth", description = "Width of the output raster")
			final Integer argOutputWidth,
			@DescribeParameter(name = "outputHeight", description = "Height of the output raster")
			final Integer argOutputHeight,
			@DescribeParameter(name = "pixelSize", description = "The pixel size to decimate by")
			final Double pixelSize )
			throws ProcessException {
		// vector-to-vector render transform that is just a pass through - the
		// key is to add map to screen transform within invertQuery
		return features;
	}

	public Query invertQuery(
			@DescribeParameter(name = "outputBBOX", description = "Georeferenced bounding box of the output")
			final ReferencedEnvelope argOutputEnv,
			@DescribeParameter(name = "outputWidth", description = "Width of the output raster")
			final Integer argOutputWidth,
			@DescribeParameter(name = "outputHeight", description = "Height of the output raster")
			final Integer argOutputHeight,
			@DescribeParameter(name = "pixelSize", description = "The pixel size to decimate by")
			final Double pixelSize,
			final Query targetQuery,
			final GridGeometry targetGridGeometry )
			throws ProcessException {

		// add to the query hints
		targetQuery.getHints().put(
				OUTPUT_WIDTH,
				argOutputWidth);
		targetQuery.getHints().put(
				OUTPUT_HEIGHT,
				argOutputHeight);
		targetQuery.getHints().put(
				OUTPUT_BBOX,
				argOutputEnv);
		if (pixelSize != null) {
			targetQuery.getHints().put(
					PIXEL_SIZE,
					pixelSize);
		}
		return targetQuery;
	}

}
