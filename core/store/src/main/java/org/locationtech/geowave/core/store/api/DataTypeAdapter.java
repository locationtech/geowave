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

import org.locationtech.geowave.core.index.ByteArray;
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
	 * Return the data adapter's type name. This also must be unique within a
	 * datastore.
	 *
	 * @return the type name which serves as a unique identifier for this
	 *         adapter
	 */
	public String getTypeName();

	/**
	 * Get a data ID for the entry
	 *
	 * @param entry
	 *            the entry
	 * @return the data ID
	 */
	public ByteArray getDataId(
			T entry );

	/**
	 * Decode GeoWave persistence payload into the entry
	 *
	 * @param data
	 *            the persistence payload
	 * @param index
	 *            the index this is coming from
	 * @return the entry
	 */
	public T decode(
			IndexedAdapterPersistenceEncoding data,
			Index index );

	/**
	 * Encode the entry into the GeoWave persistence encoding payload
	 *
	 * @param entry
	 *            the entry
	 * @param indexModel
	 *            the index model this is going into
	 * @return the persistence encoding payload representing this entry
	 */
	public AdapterPersistenceEncoding encode(
			T entry,
			CommonIndexModel indexModel );

	/**
	 * for efficiency we assume fields have some pre-defined ordering. If the
	 * field belongs in the common index model it should be one of the first n
	 * positions where n is the number of fields in the common index model.
	 *
	 * @param model
	 *            the index model
	 * @param fieldName
	 *            the field name
	 * @return the position
	 */
	public int getPositionOfOrderedField(
			CommonIndexModel model,
			String fieldName );

	/**
	 * Get the field name for the position
	 *
	 * @param model
	 *            the index model
	 * @param position
	 *            the position
	 * @return the field name, if its part of the common index model, it should
	 *         be the common field name
	 */
	public String getFieldNameForPosition(
			CommonIndexModel model,
			int position );
}
