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
package mil.nga.giat.geowave.core.ingest.index;

import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface IndexProvider
{

	/**
	 * Get an array of indices that are required by this ingest implementation.
	 * This should be a subset of supported indices. All of these indices will
	 * automatically be persisted with GeoWave's metadata store (and in the job
	 * configuration if run as a job), whereas indices that are just "supported"
	 * will not automatically be persisted (only if they are the primary index).
	 * This is primarily useful if there is a supplemental index required by the
	 * ingest process that is not the primary index.
	 * 
	 * @return the array of indices that are supported by this ingest
	 *         implementation
	 */
	public PrimaryIndex[] getRequiredIndices();
}
