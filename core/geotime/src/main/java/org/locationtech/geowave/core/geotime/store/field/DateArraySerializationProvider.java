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
package org.locationtech.geowave.core.geotime.store.field;

import java.util.Date;

import org.locationtech.geowave.core.geotime.store.field.DateSerializationProvider.DateReader;
import org.locationtech.geowave.core.geotime.store.field.DateSerializationProvider.DateWriter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import org.locationtech.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;

public class DateArraySerializationProvider implements
		FieldSerializationProviderSpi<Date[]>
{
	@Override
	public FieldReader<Date[]> getFieldReader() {
		return new DateArrayReader();
	}

	@Override
	public FieldWriter<Object, Date[]> getFieldWriter() {
		return new DateArrayWriter();
	}

	private static class DateArrayReader extends
			FixedSizeObjectArrayReader<Date>
	{
		public DateArrayReader() {
			super(
					new DateReader());
		}
	}

	private static class DateArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Date>
	{
		public DateArrayWriter() {
			super(
					new DateWriter());
		}
	}
}
