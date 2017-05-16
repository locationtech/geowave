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
package org.geotools.renderer.lite;

import java.awt.Composite;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.process.function.ProcessFunction;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.adapter.vector.plugin.DistributedRenderProcess;
import mil.nga.giat.geowave.adapter.vector.render.DistributedRenderOptions;
import mil.nga.giat.geowave.adapter.vector.render.DistributedRenderResult;
import mil.nga.giat.geowave.adapter.vector.render.InternalDistributedRenderProcess;
import mil.nga.giat.geowave.adapter.vector.render.DistributedRenderResult.CompositeGroupResult;
import mil.nga.giat.geowave.adapter.vector.render.PersistableComposite;
import mil.nga.giat.geowave.adapter.vector.render.PersistableRenderedImage;

public class DistributedRenderer extends
		StreamingRenderer
{
	private final DistributedRenderOptions options;
	protected DistributedRenderingBlockingQueue renderQueue;

	public DistributedRenderer(
			final DistributedRenderOptions options ) {
		this.options = options;
	}

	@Override
	List<List<LiteFeatureTypeStyle>> classifyByFeatureProduction(
			final List<LiteFeatureTypeStyle> lfts ) {
		// strip off a distributed rendering render transform because that is
		// what is currently being processed
		final List<List<LiteFeatureTypeStyle>> retVal = super.classifyByFeatureProduction(lfts);
		for (final List<LiteFeatureTypeStyle> featureTypeStyles : retVal) {
			final LiteFeatureTypeStyle transformLfts = featureTypeStyles.get(0);
			// there doesn't seem to be an easy way to check if its a
			// distributed render transform so for now let's just not allow
			// other rendering transformations when distributed rendering is
			// employed and strip all transformations
			if (transformLfts.transformation instanceof ProcessFunction) {
				if ((((ProcessFunction) transformLfts.transformation).getName() != null)
						&& ((ProcessFunction) transformLfts.transformation).getName().equals(
								DistributedRenderProcess.PROCESS_NAME)) {
					transformLfts.transformation = null;
				}
			}
		}
		return retVal;

	}

	@Override
	public void setRendererHints(
			final Map hints ) {
		hints.put(
				"maxFiltersToSendToDatastore",
				options.getMaxFilters());

		hints.put(
				StreamingRenderer.LINE_WIDTH_OPTIMIZATION_KEY,
				options.isOptimizeLineWidth());
		super.setRendererHints(hints);
	}

	@Override
	protected BlockingQueue<RenderingRequest> getRequestsQueue() {
		renderQueue = new DistributedRenderingBlockingQueue(
				10000);
		return renderQueue;
	}

	public DistributedRenderResult getResult(
			final BufferedImage parentImage ) {
		return renderQueue.getResult(parentImage);
	}

	public class DistributedRenderingBlockingQueue extends
			RenderingBlockingQueue
	{
		private static final long serialVersionUID = -1014302908773318665L;
		private final Map<Graphics2D, List<Pair<BufferedImage, Composite>>> compositeGroupGraphicsToStyleGraphicsMapping = new LinkedHashMap<>();
		private final Map<Graphics2D, Composite> compositeGroupGraphicsToCompositeMapping = new HashMap<>();

		public DistributedRenderingBlockingQueue(
				final int capacity ) {
			super(
					capacity);
		}

		@Override
		public void put(
				final RenderingRequest e )
				throws InterruptedException {
			// for merge requests just collect the graphics objects and
			// associated composites
			if (e instanceof MergeLayersRequest) {
				final List<LiteFeatureTypeStyle> lftsList = ((MergeLayersRequest) e).lfts;
				final List<Pair<BufferedImage, Composite>> styleGraphics = new ArrayList<>();
				final Graphics2D parentGraphics = ((MergeLayersRequest) e).graphics;
				for (final LiteFeatureTypeStyle lfts : lftsList) {
					if ((lfts.graphics instanceof DelayedBackbufferGraphic) && (lfts.graphics != parentGraphics)) {
						final DelayedBackbufferGraphic styleGraphic = (DelayedBackbufferGraphic) lfts.graphics;
						if (styleGraphic.image != null) {
							styleGraphics.add(Pair.of(
									styleGraphic.image,
									lfts.composite));
							continue;
						}
					}
					// if no style graphic was added, add a null value as a
					// placeholder in the list
					styleGraphics.add(null);
				}
				compositeGroupGraphicsToStyleGraphicsMapping.put(
						parentGraphics,
						styleGraphics);
			}
			else if (e instanceof MargeCompositingGroupRequest) {
				compositeGroupGraphicsToCompositeMapping.put(
						((MargeCompositingGroupRequest) e).compositingGroup.graphics,
						((MargeCompositingGroupRequest) e).compositingGroup.composite);
			}
			else {
				super.put(e);
			}
		}

		public DistributedRenderResult getResult(
				final BufferedImage parentImage ) {
			final List<CompositeGroupResult> compositeGroups = new ArrayList<>();
			for (final Entry<Graphics2D, List<Pair<BufferedImage, Composite>>> e : compositeGroupGraphicsToStyleGraphicsMapping
					.entrySet()) {
				final Graphics2D compositeGroupGraphic = e.getKey();
				final List<Pair<PersistableRenderedImage, PersistableComposite>> orderedStyles = Lists
						.transform(
								e.getValue(),
								new Function<Pair<BufferedImage, Composite>, Pair<PersistableRenderedImage, PersistableComposite>>() {

									@Override
									public Pair<PersistableRenderedImage, PersistableComposite> apply(
											final Pair<BufferedImage, Composite> input ) {
										if (input == null) {
											return null;
										}
										return Pair.of(
												new PersistableRenderedImage(
														input.getKey()),
												input.getValue() == null ? null : new PersistableComposite(
														input.getValue()));
									}
								});
				if (compositeGroupGraphic instanceof DelayedBackbufferGraphic) {
					final Composite compositeGroupComposite = compositeGroupGraphicsToCompositeMapping
							.get(compositeGroupGraphic);
					// because mergelayers wasn't writing to the composite
					// image, their won't be an image to persist
					final PersistableComposite persistableCGC = compositeGroupComposite == null ? null
							: new PersistableComposite(
									compositeGroupComposite);
					compositeGroups.add(new CompositeGroupResult(
							persistableCGC,
							orderedStyles));
				}
				else {
					// it must be the parent image
					compositeGroups.add(new CompositeGroupResult(
							null,
							orderedStyles));
				}
			}

			return new DistributedRenderResult(
					new PersistableRenderedImage(
							parentImage),
					compositeGroups);
		}
	}
}
