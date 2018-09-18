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

import org.locationtech.geowave.core.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import org.locationtech.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.ArrayField;
import org.locationtech.geowave.core.store.dimension.ArrayWrapper;
import org.locationtech.geowave.core.store.dimension.ArrayWrapperReader;
import org.locationtech.geowave.core.store.dimension.ArrayWrapperWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;

public class TimeArrayField extends
		ArrayField<Time> implements
		NumericDimensionField<ArrayWrapper<Time>>
{
	private ArrayWrapperReader<Time> reader;
	private ArrayWrapperWriter<Time> writer;

	public TimeArrayField(
			final NumericDimensionField<Time> elementField ) {
		super(
				elementField);
		reader = new ArrayWrapperReader<>(
				new FixedSizeObjectArrayReader<>(
						elementField.getReader()));
		writer = new ArrayWrapperWriter<Time>(
				new FixedSizeObjectArrayWriter(
						elementField.getWriter()));
	}

	public TimeArrayField() {}

	@Override
	public FieldWriter<?, ArrayWrapper<Time>> getWriter() {
		return writer;
	}

	@Override
	public FieldReader<ArrayWrapper<Time>> getReader() {
		return reader;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		super.fromBinary(bytes);
		reader = new ArrayWrapperReader<>(
				new FixedSizeObjectArrayReader<>(
						elementField.getReader()));
		writer = new ArrayWrapperWriter<Time>(
				new FixedSizeObjectArrayWriter(
						elementField.getWriter()));
	}
}
