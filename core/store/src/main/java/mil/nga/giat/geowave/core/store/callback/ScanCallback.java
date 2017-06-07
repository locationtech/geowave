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
package mil.nga.giat.geowave.core.store.callback;

import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;

/**
 * This interface provides a callback mechanism when scanning entries
 * 
 * @param <T>
 *            A generic type for ingested entries
 */
public interface ScanCallback<T>
{
	/**
	 * This will be called after an entry is successfully scanned with the row
	 * IDs that were used. Deduplication, if performed, occurs prior to calling
	 * this method.
	 * 
	 * Without or without de-duplication, row ids are not consolidate, thus each
	 * entry only contains one row id. If the entry is not de-dupped, then the
	 * entry this method is called for each duplicate, each with a different row
	 * id.
	 * 
	 * @param entryInfo
	 *            information regarding what was scanned to include the
	 *            insertion row IDs, fields, and visibilities
	 * @param entry
	 *            the entry that was ingested
	 */
	public void entryScanned(
			final DataStoreEntryInfo entryInfo,
			final T entry );
}
