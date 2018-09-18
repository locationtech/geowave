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

import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import org.locationtech.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import org.locationtech.geowave.core.store.data.field.base.DoubleSerializationProvider.DoubleReader;
import org.locationtech.geowave.core.store.data.field.base.DoubleSerializationProvider.DoubleWriter;

public class DoubleArraySerializationProvider implements
		FieldSerializationProviderSpi<Double[]>
{

	@Override
	public FieldReader<Double[]> getFieldReader() {
		return new DoubleArrayReader();
	}

	@Override
	public FieldWriter<Object, Double[]> getFieldWriter() {
		return new DoubleArrayWriter();
	}

	private static class DoubleArrayReader extends
			FixedSizeObjectArrayReader<Double>
	{
		public DoubleArrayReader() {
			super(
					new DoubleReader());
		}
	}

	private static class DoubleArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Double>
	{
		public DoubleArrayWriter() {
			super(
					new DoubleWriter());
		}
	}

}
