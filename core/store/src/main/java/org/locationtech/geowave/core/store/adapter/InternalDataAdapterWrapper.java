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
package org.locationtech.geowave.core.store.adapter;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.PrimaryIndex;

public class InternalDataAdapterWrapper<T> implements
		InternalDataAdapter<T>
{
	private WritableDataAdapter<T> adapter;
	private short internalAdapterId;

	public InternalDataAdapterWrapper(
			WritableDataAdapter<T> adapter,
			short internalAdapterId ) {
		this.adapter = adapter;
		this.internalAdapterId = internalAdapterId;
	}

	public FieldWriter<T, Object> getWriter(
			ByteArrayId fieldId ) {
		return adapter.getWriter(fieldId);
	}

	public short getInternalAdapterId() {
		return internalAdapterId;
	}

	public byte[] toBinary() {
		return adapter.toBinary();
	}

	public FieldReader<Object> getReader(
			ByteArrayId fieldId ) {
		return adapter.getReader(fieldId);
	}

	public void fromBinary(
			byte[] bytes ) {
		adapter.fromBinary(bytes);
	}

	public ByteArrayId getAdapterId() {
		return adapter.getAdapterId();
	}

	public boolean isSupported(
			T entry ) {
		return adapter.isSupported(entry);
	}

	public ByteArrayId getDataId(
			T entry ) {
		return adapter.getDataId(entry);
	}

	public T decode(
			IndexedAdapterPersistenceEncoding data,
			PrimaryIndex index ) {
		return adapter.decode(
				data,
				index);
	}

	public AdapterPersistenceEncoding encode(
			T entry,
			CommonIndexModel indexModel ) {
		AdapterPersistenceEncoding retVal = adapter.encode(
				entry,
				indexModel);
		retVal.setInternalAdapterId(internalAdapterId);
		return retVal;
	}

	public int getPositionOfOrderedField(
			CommonIndexModel model,
			ByteArrayId fieldId ) {
		return adapter.getPositionOfOrderedField(
				model,
				fieldId);
	}

	public ByteArrayId getFieldIdForPosition(
			CommonIndexModel model,
			int position ) {
		return adapter.getFieldIdForPosition(
				model,
				position);
	}

	public void init(
			PrimaryIndex... indices ) {
		adapter.init(indices);
	}

	@Override
	public WritableDataAdapter<?> getAdapter() {
		return adapter;
	}
}
