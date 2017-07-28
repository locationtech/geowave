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
package mil.nga.giat.geowave.core.ingest;

import java.net.URL;
import java.util.Collection;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;

/**
 * An interface required for ingest plugins to implement a conversion from an
 * expected input format to GeoWave data which can in turn be ingested into the
 * system.
 * 
 * @param <I>
 *            The type for the input data
 * @param <O>
 *            The type that represents each data entry being ingested
 */
public interface IngestPluginBase<I, O> extends
		DataAdapterProvider<O>
{
	/**
	 * Convert from an expected input format to a data format that can be
	 * directly ingested into GeoWave
	 * 
	 * @param file
	 *            The expected input.
	 * @param primaryIndexIds
	 *            The set of index IDs specified via a commandline argument
	 *            (this is typically either the default spatial index or default
	 *            spatial-temporal index)
	 * @param globalVisibility
	 *            If on the command-line the user specifies a global visibility
	 *            to write to the visibility column in GeoWave, it is passed
	 *            along here. It is assumed that this is the same visibility
	 *            string that will be passed to
	 *            DataAdapterProvider.getDataAdapters()
	 * @return The objects that can be directly ingested into GeoWave
	 */
	public CloseableIterator<GeoWaveData<O>> toGeoWaveData(
			I input,
			Collection<ByteArrayId> primaryIndexIds,
			String globalVisibility );
}
