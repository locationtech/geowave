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
package org.locationtech.geowave.format.geotools.vector;

import org.locationtech.geowave.core.ingest.avro.AvroFormatPlugin;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import org.locationtech.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.format.geotools.vector.retyping.date.DateFieldRetypingPlugin;
import org.opengis.feature.simple.SimpleFeature;

/**
 * This represents an ingest format plugin provider for GeoTools vector data
 * stores. It currently only supports ingesting data directly from a local file
 * system into GeoWave.
 */
public class GeoToolsVectorDataStoreIngestFormat implements
		IngestFormatPluginProviderSpi<Object, SimpleFeature>
{
	@Override
	public AvroFormatPlugin<Object, SimpleFeature> createAvroFormatPlugin(
			IngestFormatOptions options ) {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoTools vector files cannot be ingested using intermediate avro files");
	}

	@Override
	public IngestFromHdfsPlugin<Object, SimpleFeature> createIngestFromHdfsPlugin(
			IngestFormatOptions options ) {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoTools vector files cannot be ingested from HDFS");
	}

	@Override
	public LocalFileIngestPlugin<SimpleFeature> createLocalFileIngestPlugin(
			IngestFormatOptions options ) {
		GeoToolsVectorDataOptions vectorDataOptions = (GeoToolsVectorDataOptions) options;
		return new GeoToolsVectorDataStoreIngestPlugin(
				new DateFieldRetypingPlugin(
						vectorDataOptions.getDateFieldOptionProvider()),
				vectorDataOptions.getCqlFilterOptionProvider(),
				vectorDataOptions.getFeatureTypeNames());
	}

	@Override
	public String getIngestFormatName() {
		return "geotools-vector";
	}

	@Override
	public String getIngestFormatDescription() {
		return "all file-based vector datastores supported within geotools";
	}

	@Override
	public IngestFormatOptions createOptionsInstances() {
		return new GeoToolsVectorDataOptions();
	}

}
