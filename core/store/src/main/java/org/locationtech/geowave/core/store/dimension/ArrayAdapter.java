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
package org.locationtech.geowave.core.store.dimension;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.data.field.ArrayReader;
import org.locationtech.geowave.core.store.data.field.ArrayWriter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class ArrayAdapter<T> implements
		FieldReader<ArrayWrapper<T>>,
		FieldWriter<Object, ArrayWrapper<T>>
{
	protected FieldReader<T[]> reader;
	protected FieldWriter<Object, T[]> writer;

	protected ArrayAdapter() {
		super();
	}

	public ArrayAdapter(
			final ArrayReader<T> reader,
			final ArrayWriter<Object, T> writer ) {
		this.reader = reader;
		this.writer = writer;
	}

	@Override
	public byte[] getVisibility(
			final Object rowValue,
			final ByteArrayId fieldId,
			final ArrayWrapper<T> fieldValue ) {
		return fieldValue.getVisibility();
	}

	@Override
	public byte[] writeField(
			final ArrayWrapper<T> fieldValue ) {
		return writer.writeField(fieldValue.getArray());
	}

	@Override
	public ArrayWrapper<T> readField(
			final byte[] fieldData ) {
		return new ArrayWrapper<T>(
				reader.readField(fieldData));
	}

}
