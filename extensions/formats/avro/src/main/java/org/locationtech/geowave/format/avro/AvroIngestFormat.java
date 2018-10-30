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
package org.locationtech.geowave.format.avro;

import org.locationtech.geowave.adapter.vector.avro.AvroSimpleFeatureCollection;
import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestFormat;
import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;

/**
 * This represents an ingest format plugin provider for Avro data that matches
 * our generic vector avro schema. It will support ingesting directly from a
 * local file system or staging data from a local files system and ingesting
 * into GeoWave using a map-reduce job.
 */
public class AvroIngestFormat extends
		AbstractSimpleFeatureIngestFormat<AvroSimpleFeatureCollection>
{
	@Override
	protected AbstractSimpleFeatureIngestPlugin<AvroSimpleFeatureCollection> newPluginInstance(
			IngestFormatOptions options ) {
		return new AvroIngestPlugin();
	}

	@Override
	public String getIngestFormatName() {
		return "avro";
	}

	@Override
	public String getIngestFormatDescription() {
		return "This can read an Avro file encoded with the SimpleFeatureCollection schema.  This schema is also used by the export tool, so this format handles re-ingesting exported datasets.";
	}

}
