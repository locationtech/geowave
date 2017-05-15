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
import mil.nga.giat.geowave.core.store.data.field.base.LongSerializationProvider.LongReader;
import mil.nga.giat.geowave.core.store.data.field.base.LongSerializationProvider.LongWriter;

public class LongArraySerializationProvider implements
		FieldSerializationProviderSpi<Long[]>
{

	@Override
	public FieldReader<Long[]> getFieldReader() {
		return new LongArrayReader();
	}

	@Override
	public FieldWriter<Object, Long[]> getFieldWriter() {
		return new LongArrayWriter();
	}

	private static class LongArrayReader extends
			FixedSizeObjectArrayReader<Long>
	{
		public LongArrayReader() {
			super(
					new LongReader());
		}
	}

	private static class LongArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Long>
	{
		public LongArrayWriter() {
			super(
					new LongWriter());
		}
	}
}
