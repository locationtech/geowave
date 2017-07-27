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

import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader.VariableSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.base.BigIntegerSerializationProvider.BigIntegerReader;
import mil.nga.giat.geowave.core.store.data.field.base.BigIntegerSerializationProvider.BigIntegerWriter;

public class BigIntegerArraySerializationProvider implements
		FieldSerializationProviderSpi<BigInteger[]>
{
	@Override
	public FieldReader<BigInteger[]> getFieldReader() {
		return new BigIntegerArrayReader();
	}

	@Override
	public FieldWriter<Object, BigInteger[]> getFieldWriter() {
		return new BigIntegerArrayWriter();
	}

	private static class BigIntegerArrayReader extends
			VariableSizeObjectArrayReader<BigInteger>
	{
		public BigIntegerArrayReader() {
			super(
					new BigIntegerReader());
		}
	}

	private static class BigIntegerArrayWriter extends
			VariableSizeObjectArrayWriter<Object, BigInteger>
	{
		public BigIntegerArrayWriter() {
			super(
					new BigIntegerWriter());
		}
	}
}
