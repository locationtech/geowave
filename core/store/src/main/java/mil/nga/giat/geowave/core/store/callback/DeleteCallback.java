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
 * This interface provides a callback mechanism when deleting a collection of
 * entries.
 * 
 * @param <T>
 *            A generic type for ingested entries
 */
public interface DeleteCallback<T>
{
	/**
	 * This will be called after an entry is successfully ingested with the row
	 * IDs that were used
	 * 
	 * @param entryInfo
	 *            information regarding what was written to include the
	 *            insertion row IDs, fields, and visibilities
	 * @param entry
	 *            the entry that was ingested
	 */
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry );
}
