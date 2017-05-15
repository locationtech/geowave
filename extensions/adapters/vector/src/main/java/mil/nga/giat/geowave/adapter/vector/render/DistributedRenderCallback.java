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

import org.geoserver.wms.GetMapCallbackAdapter;
import org.geoserver.wms.WMS;
import org.geoserver.wms.WMSMapContent;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.map.FeatureLayer;
import org.geotools.map.Layer;
import org.geotools.process.Processors;
import org.geotools.process.function.DistributedRenderProcessUtils;
import org.geotools.process.function.ProcessFunction;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.RasterSymbolizer;
import org.geotools.styling.Rule;
import org.geotools.styling.Style;
import org.geotools.styling.StyleFactory;
import org.geotools.styling.visitor.DuplicatingStyleVisitor;
import org.opengis.filter.expression.Expression;

import mil.nga.giat.geowave.adapter.vector.plugin.DistributedRenderProcess;
import mil.nga.giat.geowave.adapter.vector.plugin.InternalProcessFactory;

/**
 * The purpose of this callback is completely to get the layer Style accessible
 * from the query, in particular making the style available to either the
 * FeatureReader or to a RenderingTransformation
 *
 */
public class DistributedRenderCallback extends
		GetMapCallbackAdapter
{
	private final WMS wms;

	public DistributedRenderCallback(
			final WMS wms ) {
		this.wms = wms;
	}

	@Override
	public Layer beforeLayer(
			final WMSMapContent mapContent,
			final Layer layer ) {
		// sanity check the style
		if ((layer instanceof FeatureLayer) && (layer.getStyle() != null)
				&& (layer.getStyle().featureTypeStyles() != null) && !layer.getStyle().featureTypeStyles().isEmpty()) {

			final Style layerStyle = layer.getStyle();
			final FeatureTypeStyle style = layerStyle.featureTypeStyles().get(
					0);
			// check if their is a DistributedRender rendering
			// transformation
			if (style instanceof ProcessFunction && style.getTransformation() != null
					&& (((ProcessFunction) style.getTransformation()).getName() != null)
					&& ((ProcessFunction) style.getTransformation()).getName().equals(
							DistributedRenderProcess.PROCESS_NAME)) {
				// if their is a DistributedRender transformation, we need
				// to provide more information that can only be found
				final DuplicatingStyleVisitor cloner = new DuplicatingStyleVisitor();
				layerStyle.accept(cloner);
				layer.getQuery().getHints().put(
						DistributedRenderProcess.OPTIONS,
						new DistributedRenderOptions(
								wms,
								mapContent,
								layerStyle));
				// now that the options with the distributed render style
				// have been set the original style will be used with
				// distributed rendering

				// now, replace the style with a direct raster symbolizer,
				// so the GridCoverage result of the distributed rendering
				// process is directly rendered to the map in place of the
				// original style

				final Style directRasterStyle = (Style) cloner.getCopy();
				directRasterStyle.featureTypeStyles().clear();
				Processors.addProcessFactory(new InternalProcessFactory());
				directRasterStyle.featureTypeStyles().add(
						getDirectRasterStyle(
								layer.getFeatureSource().getSchema().getGeometryDescriptor().getLocalName(),
								DistributedRenderProcessUtils.getRenderingProcess()));
				((FeatureLayer) layer).setStyle(directRasterStyle);
			}
		}
		return layer;
	}

	private static FeatureTypeStyle getDirectRasterStyle(
			final String geometryPropertyName,
			final Expression transformation ) {
		final StyleFactory styleFactory = CommonFactoryFinder.getStyleFactory();
		final FeatureTypeStyle style = styleFactory.createFeatureTypeStyle();
		final Rule rule = styleFactory.createRule();
		rule.setName("distributed render - direct raster");
		rule.setTitle("Distributed Render - Direct Raster");

		final RasterSymbolizer symbolizer = styleFactory.createRasterSymbolizer();
		symbolizer.setGeometryPropertyName(geometryPropertyName);
		rule.symbolizers().add(
				symbolizer);
		style.rules().add(
				rule);
		style.setTransformation(transformation);
		return style;
	}
}
