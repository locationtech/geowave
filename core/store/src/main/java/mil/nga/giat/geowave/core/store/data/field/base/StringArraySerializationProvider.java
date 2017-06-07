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
import mil.nga.giat.geowave.core.store.data.field.ArrayReader.VariableSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.base.StringSerializationProvider.StringReader;
import mil.nga.giat.geowave.core.store.data.field.base.StringSerializationProvider.StringWriter;

public class StringArraySerializationProvider implements
		FieldSerializationProviderSpi<String[]>
{

	@Override
	public FieldReader<String[]> getFieldReader() {
		return new StringArrayReader();
	}

	@Override
	public FieldWriter<Object, String[]> getFieldWriter() {
		return new StringArrayWriter();
	}

	private static class StringArrayReader extends
			VariableSizeObjectArrayReader<String>
	{
		public StringArrayReader() {
			super(
					new StringReader());
		}
	}

	private static class StringArrayWriter extends
			VariableSizeObjectArrayWriter<Object, String>
	{
		public StringArrayWriter() {
			super(
					new StringWriter());
		}
	}
}
