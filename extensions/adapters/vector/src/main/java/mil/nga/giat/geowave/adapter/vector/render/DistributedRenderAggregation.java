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

import java.awt.geom.Point2D;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.geoserver.wms.DefaultWebMapService;
import org.geoserver.wms.GetMapRequest;
import org.geoserver.wms.ScaleComputationMethod;
import org.geoserver.wms.WMSMapContent;
import org.geotools.map.FeatureLayer;
import org.geotools.map.MapViewport;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.cyclops.data.async.Queue;

import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

public class DistributedRenderAggregation implements
		Aggregation<DistributedRenderOptions, DistributedRenderResult, SimpleFeature>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DistributedRenderAggregation.class);
	private DistributedRenderMapOutputFormat currentRenderer;
	private DistributedRenderResult currentResult;
	// use a cyclops-react queue to feed simple features asynchronously while a
	// render thread consumes the features
	private Queue<SimpleFeature> queue;
	private CompletableFuture<DistributedRenderResult> asyncRenderer;
	private DistributedRenderOptions options;

	public DistributedRenderAggregation() {}

	public DistributedRenderAggregation(
			final DistributedRenderOptions options ) {
		this.options = options;
	}

	@Override
	public DistributedRenderOptions getParameters() {
		return options;
	}

	@Override
	public void setParameters(
			final DistributedRenderOptions options ) {
		this.options = options;
	}

	private void initRenderer(
			final SimpleFeatureType type ) {
		currentRenderer = new DistributedRenderMapOutputFormat(
				options);
		final WMSMapContent mapContent = new WMSMapContent();
		final GetMapRequest request = new GetMapRequest();
		mapContent.setBgColor(
				options.getBgColor());
		request.setBgColor(
				options.getBgColor());
		mapContent.setPalette(
				options.getPalette());
		request.setPalette(
				options.getPalette());
		mapContent.setAngle(
				options.getAngle());
		request.setAngle(
				options.getAngle());
		mapContent.setBuffer(
				options.getBuffer());
		request.setBuffer(
				options.getBuffer());
		mapContent.setMapWidth(
				options.getMapWidth());
		request.setWidth(
				options.getMapWidth());
		mapContent.setMapHeight(
				options.getMapHeight());
		request.setHeight(
				options.getMapHeight());
		mapContent.setTransparent(
				options.isTransparent());
		request.setTransparent(
				options.isTransparent());
		mapContent.setViewport(
				new MapViewport(
						options.getEnvelope()));
		request.setBbox(
				options.getEnvelope());
		request.setInterpolations(
				options.getInterpolations());
		final Map formatOptions = new HashMap<>();
		formatOptions.put(
				"antialias",
				options.getAntialias());
		formatOptions.put(
				"timeout",
				options.getMaxRenderTime());
		formatOptions.put(
				"kmplacemark",
				Boolean.valueOf(
						options.isKmlPlacemark()));
		// this sets a static variable, but its the only method available
		// (multiple geoserver clients with different settings hitting the same
		// distributed backend, may conflict on these settings)

		// we get around this by overriding these settings on the renderHints
		// object within DistributedRenderer so it is no longer using these
		// static settings, but these static properties must be set to avoid
		// NPEs
		System.setProperty(
				"OPTIMIZE_LINE_WIDTH",
				Boolean.toString(
						options.isOptimizeLineWidth()));
		System.setProperty(
				"MAX_FILTER_RULES",
				Integer.toString(
						options.getMaxFilters()));
		System.setProperty(
				"USE_GLOBAL_RENDERING_POOL",
				Boolean.toString(
						DistributedRenderOptions.isUseGlobalRenderPool()));
		new DefaultWebMapService(null).setApplicationContext(null);
		request.setFormatOptions(
				formatOptions);
		request.setWidth(
				options.getMapWidth());
		request.setHeight(
				options.getMapHeight());
		request.setTiled(
				options.isMetatile());
		request.setScaleMethod(
				options.isRenderScaleMethodAccurate() ? ScaleComputationMethod.Accurate : ScaleComputationMethod.OGC);

		if (options.isMetatile()) {
			// it doesn't matter what this is, as long as its not null, we are
			// just ensuring proper transparency usage based on meta-tiling
			// rules
			request.setTilesOrigin(
					new Point2D.Double());
		}
		mapContent.setRequest(
				request);
		queue = new Queue<>();
		mapContent.addLayer(
				new FeatureLayer(
						new AsyncQueueFeatureCollection(
								type,
								queue),
						options.getStyle()));
		// produce map in a separate thread...
		asyncRenderer = CompletableFuture.supplyAsync(
				() -> {
					currentRenderer.produceMap(
							mapContent).dispose();
					return currentRenderer.getDistributedRenderResult();
				});
	}

	@Override
	public DistributedRenderResult getResult() {
		if ((queue != null) && (asyncRenderer != null)) {
			queue.close();
			DistributedRenderResult result = null;
			// may not need to do this, waiting on map production may be
			// sufficient
			try {
				if (options.getMaxRenderTime() > 0) {
					result = asyncRenderer.get(
							options.getMaxRenderTime(),
							TimeUnit.SECONDS);

				}
				else {
					result = asyncRenderer.get();
				}
			}
			catch (InterruptedException | ExecutionException | TimeoutException e) {
				LOGGER.warn(
						"Unable to get distributed render result",
						e);
			}
			currentResult = result;
			clearRenderer();
		}
		return currentResult;
	}

	@Override
	public void clearResult() {
		stopRenderer();
		clearRenderer();
		currentResult = null;
	}

	public void stopRenderer() {
		if (currentRenderer != null) {
			currentRenderer.stopRendering();
		}
		if (asyncRenderer != null) {
			asyncRenderer.cancel(true);
		}
	}

	public void clearRenderer() {
		queue = null;
		currentRenderer = null;
		asyncRenderer = null;
	}

	private synchronized void ensureOpen(
			final SimpleFeatureType type ) {
		if (currentRenderer == null) {
			initRenderer(type);
		}
	}

	@Override
	public void aggregate(
			final SimpleFeature entry ) {
		ensureOpen(entry.getFeatureType());
		queue.add(entry);
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {}

}
