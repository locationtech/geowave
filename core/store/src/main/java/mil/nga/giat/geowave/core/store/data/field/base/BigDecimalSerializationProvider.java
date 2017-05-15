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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class BigDecimalSerializationProvider implements
		FieldSerializationProviderSpi<BigDecimal>
{
	@Override
	public FieldReader<BigDecimal> getFieldReader() {
		return new BigDecimalReader();
	}

	@Override
	public FieldWriter<Object, BigDecimal> getFieldWriter() {
		return new BigDecimalWriter();
	}

	protected static class BigDecimalReader implements
			FieldReader<BigDecimal>
	{
		@Override
		public BigDecimal readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 5)) {
				return null;
			}
			final ByteBuffer bb = ByteBuffer.wrap(fieldData);
			final int scale = bb.getInt();
			final byte[] unscaled = new byte[fieldData.length - 4];
			bb.get(unscaled);
			return new BigDecimal(
					new BigInteger(
							unscaled),
					scale);
		}
	}

	protected static class BigDecimalWriter implements
			FieldWriter<Object, BigDecimal>
	{
		@Override
		public byte[] writeField(
				final BigDecimal fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			final byte[] unscaled = fieldValue.unscaledValue().toByteArray();
			final ByteBuffer buf = ByteBuffer.allocate(4 + unscaled.length);
			buf.putInt(fieldValue.scale());
			buf.put(unscaled);
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final BigDecimal fieldValue ) {
			return new byte[] {};
		}
	}
}
