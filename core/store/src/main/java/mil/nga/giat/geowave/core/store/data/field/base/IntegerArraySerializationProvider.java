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

import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.base.IntegerSerializationProvider.IntegerReader;
import mil.nga.giat.geowave.core.store.data.field.base.IntegerSerializationProvider.IntegerWriter;

public class IntegerArraySerializationProvider implements
		FieldSerializationProviderSpi<Integer[]>
{

	@Override
	public FieldReader<Integer[]> getFieldReader() {
		return new IntegerArrayReader();
	}

	@Override
	public FieldWriter<Object, Integer[]> getFieldWriter() {
		return new IntegerArrayWriter();
	}

	private static class IntegerArrayReader extends
			FixedSizeObjectArrayReader<Integer>
	{
		public IntegerArrayReader() {
			super(
					new IntegerReader());
		}
	}

	private static class IntegerArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Integer>
	{
		public IntegerArrayWriter() {
			super(
					new IntegerWriter());
		}
	}

}
