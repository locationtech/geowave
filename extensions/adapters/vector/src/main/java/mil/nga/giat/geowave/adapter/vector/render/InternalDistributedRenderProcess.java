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
package mil.nga.giat.geowave.adapter.vector.render;

import java.awt.image.BufferedImage;

import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.GeoTools;
import org.geotools.factory.Hints;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.opengis.coverage.grid.GridGeometry;
import org.opengis.feature.simple.SimpleFeature;

/**
 * This class can be used as a GeoTools Render Transform
 * ('geowave:DistributedRender') within an SLD on any layer that uses the
 * GeoWave Data Store. An example SLD is provided
 * (example-slds/DecimatePoints.sld). The pixel-size allows you to skip more
 * than a single pixel. For example, a pixel size of 3 would skip an estimated
 * 3x3 pixel cell in GeoWave's row IDs. Note that rows are only skipped when a
 * feature successfully passes filters.
 *
 */
@DescribeProcess(title = "InternalDistributedRender", description = "This process will enable GeoWave to render WMS requests within the server and then this will be responsible for compositing the result client-side.")
public class InternalDistributedRenderProcess
{
	@DescribeResult(name = "result", description = "This is just a pass-through, the key is to provide enough information within invertQuery to perform a map to screen transform")
	public GridCoverage2D execute(
			@DescribeParameter(name = "data", description = "Feature collection containing the rendered image")
			final SimpleFeatureCollection features )
			throws ProcessException {
		// vector-to-raster render transform that take a single feature that
		// wraps a distributed render result and converts it to a GridCoverage2D
		if (features != null) {
			final SimpleFeatureIterator it = features.features();
			if (it.hasNext()) {
				final SimpleFeature resultFeature = features.features().next();
				final DistributedRenderResult actualResult = (DistributedRenderResult) resultFeature.getAttribute(0);
				final DistributedRenderOptions renderOptions = (DistributedRenderOptions) resultFeature.getAttribute(1);
				// convert to the GridCoverage2D required for output
				final GridCoverageFactory gcf = CoverageFactoryFinder
						.getGridCoverageFactory(GeoTools.getDefaultHints());
				final BufferedImage result = actualResult.renderComposite(renderOptions);
				final GridCoverage2D gridCov = gcf.create(
						"Process Results",
						result,
						renderOptions.getEnvelope());
				return gridCov;
			}
		}
		return null;
	}

	public Query invertQuery(
			final Query targetQuery,
			final GridGeometry targetGridGeometry )
			throws ProcessException {
		// it seems that without invertQuery returning the targetQuery, the geom
		// property field does not get set in the filter (line 205 of
		// org.geotools.renderer.lite.RenderingTransformationHelper in geotools
		// v15.1)
		return targetQuery;
	}
}
