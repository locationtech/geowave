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
import mil.nga.giat.geowave.core.store.data.field.base.FloatSerializationProvider.FloatReader;
import mil.nga.giat.geowave.core.store.data.field.base.FloatSerializationProvider.FloatWriter;

public class FloatArraySerializationProvider implements
		FieldSerializationProviderSpi<Float[]>
{

	@Override
	public FieldReader<Float[]> getFieldReader() {
		return new FloatArrayReader();
	}

	@Override
	public FieldWriter<Object, Float[]> getFieldWriter() {
		return new FloatArrayWriter();
	}

	private static class FloatArrayReader extends
			FixedSizeObjectArrayReader<Float>
	{
		public FloatArrayReader() {
			super(
					new FloatReader());
		}
	}

	private static class FloatArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Float>
	{
		public FloatArrayWriter() {
			super(
					new FloatWriter());
		}
	}
}
