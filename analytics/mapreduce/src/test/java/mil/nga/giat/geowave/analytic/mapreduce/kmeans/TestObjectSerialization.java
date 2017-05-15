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
package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

public class TestObjectSerialization implements
		Serialization<TestObject>
{

	@Override
	public boolean accept(
			final Class<?> c ) {
		return TestObject.class.isAssignableFrom(c);
	}

	@Override
	public Deserializer<TestObject> getDeserializer(
			final Class<TestObject> arg0 ) {
		return new TODeserializer();
	}

	@Override
	public Serializer<TestObject> getSerializer(
			final Class<TestObject> arg0 ) {
		return new TOSerializer();
	}

	public class TODeserializer implements
			Deserializer<TestObject>
	{

		private InputStream in;
		private DataInputStream dataInput;

		@Override
		public void open(
				final InputStream in )
				throws IOException {
			this.in = in;
			dataInput = new DataInputStream(
					in);
		}

		@Override
		public TestObject deserialize(
				final TestObject t )
				throws IOException {
			final TestObjectWritable fw = new TestObjectWritable();
			fw.readFields(dataInput);
			return (TestObject) fw.getObj();
		}

		@Override
		public void close()
				throws IOException {
			in.close();
		}

	}

	private static class TOSerializer implements
			Serializer<TestObject>
	{

		private OutputStream out;
		private DataOutput dataOutput;

		@Override
		public void open(
				final OutputStream out )
				throws IOException {
			this.out = out;
			dataOutput = new DataOutputStream(
					out);
		}

		@Override
		public void serialize(
				final TestObject t )
				throws IOException {
			final TestObjectWritable fw = new TestObjectWritable(
					t);

			fw.write(dataOutput);
		}

		@Override
		public void close()
				throws IOException {
			out.close();
		}
	}

}
