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

public class LongSerializationProvider implements
		FieldSerializationProviderSpi<Long>
{
	@Override
	public FieldReader<Long> getFieldReader() {
		return new LongReader();
	}

	@Override
	public FieldWriter<Object, Long> getFieldWriter() {
		return new LongWriter();
	}

	protected static class LongReader implements
			FieldReader<Long>
	{
		@Override
		public Long readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 8)) {
				return null;
			}
			return ByteBuffer.wrap(
					fieldData).getLong();
		}
	}

	protected static class LongWriter implements
			FieldWriter<Object, Long>
	{
		public LongWriter() {
			super();
		}

		@Override
		public byte[] writeField(
				final Long fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}

			final ByteBuffer buf = ByteBuffer.allocate(8);
			buf.putLong(fieldValue);
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Long fieldValue ) {
			return new byte[] {};
		}
	}

}
