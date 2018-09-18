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
package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.data.DataReader;
import org.locationtech.geowave.core.store.data.DataWriter;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

/**
 * This interface should be implemented by any custom data type that must be
 * stored in the GeoWave index. It enables storing and retrieving the data, as
 * well as translating the data into values and queries that can be used to
 * index. Additionally, each entry is responsible for providing visibility if
 * applicable.
 * 
 * @param <T>
 *            The type of entries that this adapter works on.
 */
public interface DataTypeAdapter<T> extends
		DataReader<Object>,
		DataWriter<T, Object>,
		Persistable
{
	/**
	 * Return the adapter ID
	 * 
	 * @return a unique identifier for this adapter
	 */
	public String getTypeName();

	public ByteArrayId getDataId(
			T entry );

	public T decode(
			IndexedAdapterPersistenceEncoding data,
			Index index );

	public AdapterPersistenceEncoding encode(
			T entry,
			CommonIndexModel indexModel );

	public int getPositionOfOrderedField(
			CommonIndexModel model,
			String fieldName );

	public String getFieldNameForPosition(
			CommonIndexModel model,
			int position );
}
