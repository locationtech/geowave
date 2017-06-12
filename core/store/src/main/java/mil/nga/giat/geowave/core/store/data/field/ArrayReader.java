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
package mil.nga.giat.geowave.core.store.data.field;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.Encoding;
import mil.nga.giat.geowave.core.store.util.GenericTypeResolver;

/**
 * This class contains the basic array reader field types
 * 
 */
public class ArrayReader<FieldType> implements
		FieldReader<FieldType[]>
{

	private final FieldReader<FieldType> reader;

	public ArrayReader(
			final FieldReader<FieldType> reader ) {
		this.reader = reader;
	}

	@Override
	public FieldType[] readField(
			final byte[] fieldData ) {

		final byte encoding = fieldData[0];

		// try to read the encoding first
		if (encoding == Encoding.FIXED_SIZE_ENCODING.getByteEncoding()) {
			return readFixedSizeField(fieldData);
		}
		else if (encoding == Encoding.VARIABLE_SIZE_ENCODING.getByteEncoding()) {
			return readVariableSizeField(fieldData);
		}

		// class type not supported!
		// to be safe, treat as variable size
		return readVariableSizeField(fieldData);
	}

	@SuppressWarnings("unchecked")
	protected FieldType[] readFixedSizeField(
			final byte[] fieldData ) {
		if (fieldData.length < 1) {
			return null;
		}

		final List<FieldType> result = new ArrayList<FieldType>();

		final ByteBuffer buff = ByteBuffer.wrap(fieldData);

		// this would be bad
		if (buff.get() != Encoding.FIXED_SIZE_ENCODING.getByteEncoding()) {
			return null;
		}

		final int bytesPerEntry = buff.getInt();

		final byte[] data = new byte[bytesPerEntry];

		while (buff.remaining() > 0) {

			final int header = buff.get();

			for (int i = 0; i < 8; i++) {

				final int mask = (int) Math.pow(
						2.0,
						i);

				if ((header & mask) != 0) {
					if (buff.remaining() > 0) {
						buff.get(data);
						result.add(reader.readField(data));
					}
					else {
						break;
					}
				}
				else {
					result.add(null);
				}
			}
		}
		final FieldType[] resultArray = (FieldType[]) Array.newInstance(
				GenericTypeResolver.resolveTypeArgument(
						reader.getClass(),
						FieldReader.class),
				result.size());
		return result.toArray(resultArray);
	}

	@SuppressWarnings("unchecked")
	protected FieldType[] readVariableSizeField(
			final byte[] fieldData ) {
		if ((fieldData == null) || (fieldData.length < 4)) {
			return null;
		}
		final List<FieldType> result = new ArrayList<FieldType>();

		final ByteBuffer buff = ByteBuffer.wrap(fieldData);

		// this would be bad
		if (buff.get() != Encoding.VARIABLE_SIZE_ENCODING.getByteEncoding()) {
			return null;
		}

		while (buff.remaining() >= 4) {
			final int size = buff.getInt();
			if (size > 0) {
				final byte[] bytes = new byte[size];
				buff.get(bytes);
				result.add(reader.readField(bytes));
			}
			else {
				result.add(null);
			}
		}
		final FieldType[] resultArray = (FieldType[]) Array.newInstance(
				GenericTypeResolver.resolveTypeArgument(
						reader.getClass(),
						FieldReader.class),
				result.size());
		return result.toArray(resultArray);
	}

	public static class FixedSizeObjectArrayReader<FieldType> extends
			ArrayReader<FieldType>
	{
		public FixedSizeObjectArrayReader(
				final FieldReader<FieldType> reader ) {
			super(
					reader);
		}

		@Override
		public FieldType[] readField(
				final byte[] fieldData ) {
			return readFixedSizeField(fieldData);
		}
	}

	public static class VariableSizeObjectArrayReader<FieldType> extends
			ArrayReader<FieldType>
	{
		public VariableSizeObjectArrayReader(
				final FieldReader<FieldType> reader ) {
			super(
					reader);
		}

		@Override
		public FieldType[] readField(
				final byte[] fieldData ) {
			return readVariableSizeField(fieldData);
		}
	}
}
