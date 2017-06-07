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
package mil.nga.giat.geowave.cli.osm.types;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import mil.nga.giat.geowave.cli.osm.types.generated.LongArray;

/**
 *
 */
public class TypeUtils
{

	private static final EncoderFactory ef = EncoderFactory.get();
	private static final DecoderFactory df = DecoderFactory.get();
	private static final Map<String, SpecificDatumWriter> writers = new HashMap<>();
	private static final Map<String, SpecificDatumReader> readers = new HashMap<>();

	private static <T> byte[] deserialize(
			final T avroObject,
			Schema avroSchema,
			Class<T> avroClass )
			throws IOException {

		ByteArrayOutputStream os = new ByteArrayOutputStream();
		BinaryEncoder encoder = ef.binaryEncoder(
				os,
				null);
		if (!writers.containsKey(avroClass.toString())) {
			writers.put(
					avroClass.toString(),
					new SpecificDatumWriter<T>(
							avroSchema));
		}

		SpecificDatumWriter<T> writer = writers.get(avroClass.toString());
		writer.write(
				avroObject,
				encoder);
		encoder.flush();
		return os.toByteArray();
	}

	private static <T> T deserialize(
			final T avroObject,
			final byte[] avroData,
			Class<T> avroClass,
			Schema avroSchema )
			throws IOException {
		BinaryDecoder decoder = df.binaryDecoder(
				avroData,
				null);
		if (!readers.containsKey(avroClass.toString())) {
			readers.put(
					avroClass.toString(),
					new SpecificDatumReader(
							avroSchema));
		}
		SpecificDatumReader<T> reader = readers.get(avroClass.toString());
		return reader.read(
				avroObject,
				decoder);
	}

	public static LongArray deserializeLongArray(
			final byte[] avroData,
			LongArray reusableInstance )
			throws IOException {
		if (reusableInstance == null) {
			reusableInstance = new LongArray();
		}
		return deserialize(
				reusableInstance,
				avroData,
				LongArray.class,
				LongArray.getClassSchema());
	}

	public static byte[] serializeLongArray(
			LongArray avroObject )
			throws IOException {
		return deserialize(
				avroObject,
				LongArray.getClassSchema(),
				LongArray.class);
	}

	/*
	 * 
	 * private static <T> byte[] encodeObject(final T datum, final
	 * GenericDatumWriter<T> writer) throws IOException { // The encoder
	 * instantiation can be replaced with a ThreadLocal if needed
	 * ByteArrayOutputStream os = new ByteArrayOutputStream(); BinaryEncoder
	 * encoder = ENCODER_FACTORY.binaryEncoder(os, null); writer.write(datum,
	 * encoder); encoder.flush(); return os.toByteArray(); }
	 * 
	 * private static <T> T decodeObject(final T object, final byte[] data,
	 * final SpecificDatumReader<T> reader) throws IOException { Decoder decoder
	 * = DECODER_FACTORY.binaryDecoder(data, null); return reader.read(object,
	 * decoder); }
	 */
}
