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
import org.locationtech.geowave.core.store.data.field.ArrayReader.VariableSizeObjectArrayReader;
import org.locationtech.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import org.locationtech.geowave.core.store.dimension.ArrayAdapter;
import org.locationtech.geowave.core.store.dimension.ArrayField;
import org.locationtech.geowave.core.store.dimension.ArrayWrapper;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;

public class SpatialArrayField extends
		ArrayField<GeometryWrapper> implements
		NumericDimensionField<ArrayWrapper<GeometryWrapper>>
{
	private ArrayAdapter<GeometryWrapper> adapter;

	public SpatialArrayField(
			final NumericDimensionField<GeometryWrapper> elementField ) {
		super(
				elementField);
		adapter = new ArrayAdapter<GeometryWrapper>(
				new VariableSizeObjectArrayReader(
						elementField.getReader()),
				new VariableSizeObjectArrayWriter(
						elementField.getWriter()));
	}

	public SpatialArrayField() {}

	@Override
	public FieldWriter<?, ArrayWrapper<GeometryWrapper>> getWriter() {
		return adapter;
	}

	@Override
	public FieldReader<ArrayWrapper<GeometryWrapper>> getReader() {
		return adapter;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		super.fromBinary(bytes);
		adapter = new ArrayAdapter<GeometryWrapper>(
				new VariableSizeObjectArrayReader(
						elementField.getReader()),
				new VariableSizeObjectArrayWriter(
						elementField.getWriter()));
	}
}
