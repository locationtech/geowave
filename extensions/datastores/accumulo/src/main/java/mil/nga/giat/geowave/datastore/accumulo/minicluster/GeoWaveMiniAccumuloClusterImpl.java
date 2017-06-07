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
package mil.nga.giat.geowave.datastore.accumulo.minicluster;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;

public class GeoWaveMiniAccumuloClusterImpl extends
		MiniAccumuloClusterImpl
{

	public GeoWaveMiniAccumuloClusterImpl(
			MiniAccumuloConfigImpl config )
			throws IOException {
		super(
				config);
	}

	public void setExternalShutdownExecutor(
			ExecutorService svc ) {
		// @formatter:off
		/*if[accumulo.api=1.6]
		// nothing here
		else[accumulo.api=1.6]*/
		setShutdownExecutor(svc);
		/*end[accumulo.api=1.6]*/
		// @formatter:on
	}
}
