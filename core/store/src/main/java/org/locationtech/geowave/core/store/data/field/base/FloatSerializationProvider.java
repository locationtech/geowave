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

public class FloatSerializationProvider implements
		FieldSerializationProviderSpi<Float>
{
	@Override
	public FieldReader<Float> getFieldReader() {
		return new FloatReader();
	}

	@Override
	public FieldWriter<Object, Float> getFieldWriter() {
		return new FloatWriter();
	}

	protected static class FloatReader implements
			FieldReader<Float>
	{
		@Override
		public Float readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 4)) {
				return null;
			}
			return ByteBuffer.wrap(
					fieldData).getFloat();
		}
	}

	protected static class FloatWriter implements
			FieldWriter<Object, Float>
	{
		@Override
		public byte[] writeField(
				final Float fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			final ByteBuffer buf = ByteBuffer.allocate(4);
			buf.putFloat(fieldValue);
			return buf.array();
		}
	}

}
