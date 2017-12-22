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
package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.store.data.DataReader;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * This interface should be implemented by any custom data type that must be
 * stored in the Accumulo index. It enables storing and retrieving the data, as
 * well as translating the data into values and queries that can be used to
 * index. Additionally, each entry is responsible for providing visibility if
 * applicable.
 * 
 * @param <T>
 *            The type for the data elements that are being adapted
 */
public interface DataAdapter<T> extends
		DataReader<Object>,
		Persistable
{
	/**
	 * Return the adapter ID
	 * 
	 * @return a unique identifier for this adapter
	 */
	public ByteArrayId getAdapterId();

	public boolean isSupported(
			T entry );

	public ByteArrayId getDataId(
			T entry );

	public T decode(
			IndexedAdapterPersistenceEncoding data,
			PrimaryIndex index );

	public AdapterPersistenceEncoding encode(
			T entry,
			CommonIndexModel indexModel );

	public int getPositionOfOrderedField(
			CommonIndexModel model,
			ByteArrayId fieldId );

	public ByteArrayId getFieldIdForPosition(
			CommonIndexModel model,
			int position );

	public void init(
			PrimaryIndex... indices );
}
