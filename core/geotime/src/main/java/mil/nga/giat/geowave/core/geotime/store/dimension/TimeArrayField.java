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
package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.ArrayAdapter;
import mil.nga.giat.geowave.core.store.dimension.ArrayField;
import mil.nga.giat.geowave.core.store.dimension.ArrayWrapper;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;

public class TimeArrayField extends
		ArrayField<Time> implements
		NumericDimensionField<ArrayWrapper<Time>>
{
	private ArrayAdapter<Time> adapter;

	public TimeArrayField(
			final NumericDimensionField<Time> elementField ) {
		super(
				elementField);
		adapter = new ArrayAdapter<Time>(
				new FixedSizeObjectArrayReader(
						elementField.getReader()),
				new FixedSizeObjectArrayWriter(
						elementField.getWriter()));
	}

	public TimeArrayField() {}

	@Override
	public FieldWriter<?, ArrayWrapper<Time>> getWriter() {
		return adapter;
	}

	@Override
	public FieldReader<ArrayWrapper<Time>> getReader() {
		return adapter;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		super.fromBinary(bytes);
		adapter = new ArrayAdapter<Time>(
				new FixedSizeObjectArrayReader(
						elementField.getReader()),
				new FixedSizeObjectArrayWriter(
						elementField.getWriter()));
	}
}
