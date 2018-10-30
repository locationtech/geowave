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
package org.locationtech.geowave.core.store.data.field.base;

import java.nio.ByteBuffer;

import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class DoubleSerializationProvider implements
		FieldSerializationProviderSpi<Double>
{
	@Override
	public FieldReader<Double> getFieldReader() {
		return new DoubleReader();
	}

	@Override
	public FieldWriter<Object, Double> getFieldWriter() {
		return new DoubleWriter();
	}

	protected static class DoubleReader implements
			FieldReader<Double>
	{
		@Override
		public Double readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 8)) {
				return null;
			}
			return ByteBuffer.wrap(
					fieldData).getDouble();
		}
	}

	protected static class DoubleWriter implements
			FieldWriter<Object, Double>
	{
		@Override
		public byte[] writeField(
				final Double fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}

			final ByteBuffer buf = ByteBuffer.allocate(8);
			buf.putDouble(fieldValue);
			return buf.array();
		}
	}
}
