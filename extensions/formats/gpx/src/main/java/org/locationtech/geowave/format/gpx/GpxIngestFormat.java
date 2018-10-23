/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.format.gpx;

import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestFormat;
import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import org.locationtech.geowave.adapter.vector.ingest.GeometrySimpOptionProvider;
import org.locationtech.geowave.adapter.vector.ingest.SimpleFeatureIngestOptions;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;

import com.beust.jcommander.ParametersDelegate;

/**
 * This represents an ingest format plugin provider for GPX data. It will
 * support ingesting directly from a local file system or staging data from a
 * local files system and ingesting into GeoWave using a map-reduce job.
 */
public class GpxIngestFormat extends
		AbstractSimpleFeatureIngestFormat<GpxTrack>
{
	private final MaxExtentOptProvider extentOptProvider = new MaxExtentOptProvider();

	@Override
	protected AbstractSimpleFeatureIngestPlugin<GpxTrack> newPluginInstance(
			IngestFormatOptions options ) {
		GpxIngestPlugin plugin = new GpxIngestPlugin();
		plugin.setExtentOptionProvider(extentOptProvider);
		return plugin;
	}

	@Override
	public String getIngestFormatName() {
		return "gpx";
	}

	@Override
	public String getIngestFormatDescription() {
		return "xml files adhering to the schema of gps exchange format";
	}

	@Override
	protected Object internalGetIngestFormatOptionProviders() {
		return extentOptProvider;
	}

}
