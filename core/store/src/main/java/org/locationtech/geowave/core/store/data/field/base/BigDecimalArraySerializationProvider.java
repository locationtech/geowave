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

import java.math.BigDecimal;

import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.field.ArrayReader.VariableSizeObjectArrayReader;
import org.locationtech.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import org.locationtech.geowave.core.store.data.field.base.BigDecimalSerializationProvider.BigDecimalReader;
import org.locationtech.geowave.core.store.data.field.base.BigDecimalSerializationProvider.BigDecimalWriter;

public class BigDecimalArraySerializationProvider implements
		FieldSerializationProviderSpi<BigDecimal[]>
{
	@Override
	public FieldReader<BigDecimal[]> getFieldReader() {
		return new BigDecimalArrayReader();
	}

	@Override
	public FieldWriter<Object, BigDecimal[]> getFieldWriter() {
		return new BigDecimalArrayWriter();
	}

	private static class BigDecimalArrayReader extends
			VariableSizeObjectArrayReader<BigDecimal>
	{
		public BigDecimalArrayReader() {
			super(
					new BigDecimalReader());
		}
	}

	private static class BigDecimalArrayWriter extends
			VariableSizeObjectArrayWriter<Object, BigDecimal>
	{
		public BigDecimalArrayWriter() {
			super(
					new BigDecimalWriter());
		}
	}
}
