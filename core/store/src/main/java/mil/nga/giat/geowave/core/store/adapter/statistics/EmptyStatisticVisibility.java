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
package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;

/**
 * 
 * Supplies not additional visibility
 * 
 * @param <T>
 */
public class EmptyStatisticVisibility<T> implements
		EntryVisibilityHandler<T>
{

	@Override
	public byte[] getVisibility(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		return new byte[0];
	}

}
