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
package mil.nga.giat.geowave.core.store.data.field.base;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class PrimitiveFloatArraySerializationProvider implements
		FieldSerializationProviderSpi<float[]>
{
	@Override
	public FieldReader<float[]> getFieldReader() {
		return new PrimitiveFloatArrayReader();
	}

	@Override
	public FieldWriter<Object, float[]> getFieldWriter() {
		return new PrimitiveFloatArrayWriter();
	}

	private static class PrimitiveFloatArrayReader implements
			FieldReader<float[]>
	{
		@Override
		public float[] readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 4)) {
				return null;
			}
			final FloatBuffer buff = ByteBuffer.wrap(
					fieldData).asFloatBuffer();
			final float[] result = new float[buff.remaining()];
			buff.get(result);
			return result;
		}
	}

	private static class PrimitiveFloatArrayWriter implements
			FieldWriter<Object, float[]>
	{
		@Override
		public byte[] writeField(
				final float[] fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			final ByteBuffer buf = ByteBuffer.allocate(4 * fieldValue.length);
			for (final float value : fieldValue) {
				buf.putFloat(value);
			}
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final float[] fieldValue ) {
			return new byte[] {};
		}
	}
}
