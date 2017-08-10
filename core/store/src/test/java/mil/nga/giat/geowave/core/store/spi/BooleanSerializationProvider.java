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
package mil.nga.giat.geowave.core.store.spi;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.SPIServiceRegistry;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

/**
 * Sample Provider to test {@link SPIServiceRegistry}
 * 
 */
public class BooleanSerializationProvider implements
		FieldSerializationProviderSpi<Boolean>
{
	@Override
	public FieldReader<Boolean> getFieldReader() {
		return new BooleanReader();
	}

	@Override
	public FieldWriter<Object, Boolean> getFieldWriter() {
		return new BooleanWriter();
	}

	protected static class BooleanReader implements
			FieldReader<Boolean>
	{
		@Override
		public Boolean readField(
				final byte[] fieldData ) {
			if (fieldData == null) {
				return null;
			}
			return fieldData[0] > 0 ? Boolean.TRUE : Boolean.FALSE;
		}
	}

	protected static class BooleanWriter implements
			FieldWriter<Object, Boolean>
	{
		@Override
		public byte[] writeField(
				final Boolean fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(4);
			buf.put(fieldValue.booleanValue() ? (byte) 0 : (byte) 0x1);
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Boolean fieldValue ) {
			return new byte[] {};
		}
	}

}
