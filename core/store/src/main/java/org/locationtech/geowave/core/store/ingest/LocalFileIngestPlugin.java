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
package org.locationtech.geowave.core.store.ingest;

import java.net.URL;

/**
 * This is the primary plugin for directly ingesting data to GeoWave from local
 * files. It will write any GeoWaveData that is emitted for any supported file.
 * 
 * 
 * @param <O>
 *            The type of data to write to GeoWave
 */
public interface LocalFileIngestPlugin<O> extends
		LocalPluginBase,
		IngestPluginBase<URL, O>,
		IndexProvider
{
}
