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
import mil.nga.giat.geowave.core.store.data.field.base.ShortSerializationProvider.ShortReader;
import mil.nga.giat.geowave.core.store.data.field.base.ShortSerializationProvider.ShortWriter;

public class ShortArraySerializationProvider implements
		FieldSerializationProviderSpi<Short[]>
{
	@Override
	public FieldReader<Short[]> getFieldReader() {
		return new ShortArrayReader();
	}

	@Override
	public FieldWriter<Object, Short[]> getFieldWriter() {
		return new ShortArrayWriter();
	}

	private static class ShortArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Short>
	{
		public ShortArrayWriter() {
			super(
					new ShortWriter());
		}
	}

	private static class ShortArrayReader extends
			FixedSizeObjectArrayReader<Short>
	{
		public ShortArrayReader() {
			super(
					new ShortReader());
		}
	}
}
