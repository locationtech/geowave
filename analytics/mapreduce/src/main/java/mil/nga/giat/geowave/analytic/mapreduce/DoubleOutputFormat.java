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
package mil.nga.giat.geowave.analytic.mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DoubleOutputFormat<K, V> extends
		FileOutputFormat<K, V>
{
	protected static class DoubleRecordWriter<K, V> extends
			RecordWriter<K, V>
	{
		protected DataOutputStream out;

		public DoubleRecordWriter(
				final DataOutputStream out ) {
			super();
			this.out = out;
		}

		@Override
		public synchronized void write(
				final K key,
				final V value )
				throws IOException {
			if ((value != null) && !(value instanceof NullWritable)) {
				out.writeDouble(((DoubleWritable) value).get());
			}
		}

		@Override
		public synchronized void close(
				final TaskAttemptContext context )
				throws IOException {
			out.close();
		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(
			final TaskAttemptContext job )
			throws IOException,
			InterruptedException {
		final Configuration conf = job.getConfiguration();

		final Path file = getDefaultWorkFile(
				job,
				"");
		final FileSystem fs = file.getFileSystem(conf);

		final FSDataOutputStream fileOut = fs.create(
				file,
				false);
		return new DoubleRecordWriter<K, V>(
				fileOut);

	}

}
