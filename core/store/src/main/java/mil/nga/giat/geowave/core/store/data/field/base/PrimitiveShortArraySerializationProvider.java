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
import java.nio.ShortBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class PrimitiveShortArraySerializationProvider implements
		FieldSerializationProviderSpi<short[]>
{
	@Override
	public FieldReader<short[]> getFieldReader() {
		return new PrimitiveShortArrayReader();
	}

	@Override
	public FieldWriter<Object, short[]> getFieldWriter() {
		return new PrimitiveShortArrayWriter();
	}

	private static class PrimitiveShortArrayReader implements
			FieldReader<short[]>
	{

		@Override
		public short[] readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 2)) {
				return null;
			}
			final ShortBuffer buff = ByteBuffer.wrap(
					fieldData).asShortBuffer();
			final short[] result = new short[buff.remaining()];
			buff.get(result);
			return result;
		}
	}

	private static class PrimitiveShortArrayWriter implements
			FieldWriter<Object, short[]>
	{
		@Override
		public byte[] writeField(
				final short[] fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}

			final ByteBuffer buf = ByteBuffer.allocate(2 * fieldValue.length);
			for (final short value : fieldValue) {
				buf.putShort(value);
			}
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final short[] fieldValue ) {
			return new byte[] {};
		}
	}

}
