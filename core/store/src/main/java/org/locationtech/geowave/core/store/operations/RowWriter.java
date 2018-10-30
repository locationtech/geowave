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
package org.locationtech.geowave.core.store.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;

/**
 * This interface is returned by DataStoreOperations and useful for general
 * purpose writing of entries. The default implementation of AccumuloOperations
 * will wrap this interface with a BatchWriter but can be overridden for other
 * mechanisms to write the data.
 */
public interface RowWriter extends
		AutoCloseable
{
	public void write(
			GeoWaveRow[] rows );

	public void write(
			GeoWaveRow row );

	public void flush();
}
