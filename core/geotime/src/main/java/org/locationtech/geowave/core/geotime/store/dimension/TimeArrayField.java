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
package org.locationtech.geowave.core.geotime.store.dimension;

import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import org.locationtech.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import org.locationtech.geowave.core.store.dimension.ArrayAdapter;
import org.locationtech.geowave.core.store.dimension.ArrayField;
import org.locationtech.geowave.core.store.dimension.ArrayWrapper;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;

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
