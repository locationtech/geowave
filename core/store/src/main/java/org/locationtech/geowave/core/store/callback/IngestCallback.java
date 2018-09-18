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
package org.locationtech.geowave.core.store.callback;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;

/**
 * This interface provides a callback mechanism when ingesting a collection of
 * entries to receive the row IDs where each entry is ingested
 *
 * @param <T>
 *            A generic type for ingested entries
 */
public interface IngestCallback<T>
{
	/**
	 * This will be called after an entry is successfully ingested with the row
	 * IDs that were used
	 *
	 * @param entry
	 *            the entry that was ingested
	 * @param entryInfo
	 *            information regarding what was written to include the
	 *            insertion row IDs, fields, and visibilities
	 * @param rows
	 *            the rows inserted into the table for this entry
	 */
	public void entryIngested(
			T entry,
			GeoWaveRow... rows );

}
