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

import org.geotools.process.factory.AnnotatedBeanProcessFactory;
import org.geotools.text.Text;

import mil.nga.giat.geowave.adapter.vector.render.InternalDistributedRenderProcess;

public class InternalProcessFactory extends
		AnnotatedBeanProcessFactory
{

	public InternalProcessFactory() {
		super(
				Text.text("Internal GeoWave Process Factory"),
				"internal",
				InternalDistributedRenderProcess.class);
	}

	@Override
	public boolean isAvailable() {
		return true;
	}

}
