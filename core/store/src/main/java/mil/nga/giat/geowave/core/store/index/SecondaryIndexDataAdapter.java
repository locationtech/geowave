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
package mil.nga.giat.geowave.core.store.index;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

/**
 * This interface allows for a data adapter to define a set of secondary indices
 * 
 * @param <T>
 *            The type for the data element that is being adapted
 * 
 */
public interface SecondaryIndexDataAdapter<T> extends
		DataAdapter<T>
{
	public List<SecondaryIndex<T>> getSupportedSecondaryIndices();

	public EntryVisibilityHandler<T> getVisibilityHandler(
			ByteArrayId indexId );
}
