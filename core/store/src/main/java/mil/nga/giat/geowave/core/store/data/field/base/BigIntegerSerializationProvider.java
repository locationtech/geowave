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

import java.math.BigInteger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class BigIntegerSerializationProvider implements
		FieldSerializationProviderSpi<BigInteger>
{
	@Override
	public FieldReader<BigInteger> getFieldReader() {
		return new BigIntegerReader();
	}

	@Override
	public FieldWriter<Object, BigInteger> getFieldWriter() {
		return new BigIntegerWriter();
	}

	protected static class BigIntegerReader implements
			FieldReader<BigInteger>
	{
		@Override
		public BigInteger readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 4)) {
				return null;
			}
			return new BigInteger(
					fieldData);
		}
	}

	protected static class BigIntegerWriter implements
			FieldWriter<Object, BigInteger>
	{
		@Override
		public byte[] writeField(
				final BigInteger fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			return fieldValue.toByteArray();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final BigInteger fieldValue ) {
			return new byte[] {};
		}
	}
}
