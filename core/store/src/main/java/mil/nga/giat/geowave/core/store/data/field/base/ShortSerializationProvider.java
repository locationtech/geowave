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

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class ShortSerializationProvider implements
		FieldSerializationProviderSpi<Short>
{

	@Override
	public FieldReader<Short> getFieldReader() {
		return new ShortReader();
	}

	@Override
	public FieldWriter<Object, Short> getFieldWriter() {
		return new ShortWriter();
	}

	protected static class ShortReader implements
			FieldReader<Short>
	{
		@Override
		public Short readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 2)) {
				return null;
			}
			return ByteBuffer.wrap(
					fieldData).getShort();
		}
	}

	protected static class ShortWriter implements
			FieldWriter<Object, Short>
	{
		@Override
		public byte[] writeField(
				final Short fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}

			final ByteBuffer buf = ByteBuffer.allocate(2);
			buf.putShort(fieldValue);
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Short fieldValue ) {
			return new byte[] {};
		}
	}
}
