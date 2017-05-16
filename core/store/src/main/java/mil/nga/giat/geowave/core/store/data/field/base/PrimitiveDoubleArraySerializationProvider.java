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

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class PrimitiveDoubleArraySerializationProvider implements
		FieldSerializationProviderSpi<double[]>
{
	@Override
	public FieldReader<double[]> getFieldReader() {
		return new PrimitiveDoubleArrayReader();
	}

	@Override
	public FieldWriter<Object, double[]> getFieldWriter() {
		return new PrimitiveDoubleArrayWriter();
	}

	private static class PrimitiveDoubleArrayReader implements
			FieldReader<double[]>
	{

		@Override
		public double[] readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 8)) {
				return null;
			}
			final DoubleBuffer buff = ByteBuffer.wrap(
					fieldData).asDoubleBuffer();
			final double[] result = new double[buff.remaining()];
			buff.get(result);
			return result;
		}
	}

	private static class PrimitiveDoubleArrayWriter implements
			FieldWriter<Object, double[]>
	{
		@Override
		public byte[] writeField(
				final double[] fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			final ByteBuffer buf = ByteBuffer.allocate(8 * fieldValue.length);
			for (final double value : fieldValue) {
				buf.putDouble(value);
			}
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final double[] fieldValue ) {
			return new byte[] {};
		}
	}
}
