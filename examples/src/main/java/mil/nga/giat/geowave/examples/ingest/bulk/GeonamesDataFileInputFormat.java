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
package mil.nga.giat.geowave.examples.ingest.bulk;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * GeoNames provides exports by country (see <a
 * href="http://download.geonames.org/export/dump/"
 * >http://download.geonames.org/export/dump/</a>). These files contain one
 * tab-delimited entry per line.
 */
public class GeonamesDataFileInputFormat extends
		FileInputFormat<LongWritable, Text>
{

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split,
			TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		return new LineRecordReader();
	}

}
