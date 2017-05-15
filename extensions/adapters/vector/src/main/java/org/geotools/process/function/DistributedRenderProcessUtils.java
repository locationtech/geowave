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
package org.geotools.process.function;

import java.util.Collections;
import java.util.Map;

import org.geotools.data.Parameter;
import org.geotools.feature.NameImpl;
import org.geotools.filter.LiteralExpressionImpl;
import org.geotools.process.ProcessFactory;
import org.geotools.process.Processors;
import org.geotools.process.RenderingProcess;
import org.geotools.process.factory.AnnotatedBeanProcessFactory;
import org.geotools.text.Text;
import org.opengis.feature.type.Name;
import org.opengis.filter.expression.Expression;

import mil.nga.giat.geowave.adapter.vector.plugin.InternalProcessFactory;
import mil.nga.giat.geowave.adapter.vector.render.InternalDistributedRenderProcess;

public class DistributedRenderProcessUtils
{
	private static Expression SINGLETON_RENDER_PROCESS = null;

	public static Expression getRenderingProcess() {
		if (SINGLETON_RENDER_PROCESS == null) {
			final ProcessFactory processFactory = new AnnotatedBeanProcessFactory(
					Text.text("Internal GeoWave Process Factory"),
					"internal",
					InternalDistributedRenderProcess.class);
			final Name processName = new NameImpl(
					"internal",
					"InternalDistributedRender");
			final RenderingProcess process = (RenderingProcess) processFactory.create(processName);
			final Map<String, Parameter<?>> parameters = processFactory.getParameterInfo(processName);
			final InternalProcessFactory factory = new InternalProcessFactory();
			// this is kinda a hack, but the only way to instantiate a process
			// is
			// for it to have a registered process factory, so temporarily
			// register
			// the process factory
			Processors.addProcessFactory(factory);

			SINGLETON_RENDER_PROCESS = new RenderingProcessFunction(
					processName,
					Collections.singletonList(new ParameterFunction(
							null,
							Collections.singletonList(new LiteralExpressionImpl(
									"data")))),
					parameters,
					process,
					null);
			Processors.removeProcessFactory(factory);
		}
		return SINGLETON_RENDER_PROCESS;
	}
}
