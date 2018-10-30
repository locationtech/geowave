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
package org.locationtech.geowave.core.ingest.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.locationtech.geowave.core.ingest.avro.AvroFormatPlugin;
import org.locationtech.geowave.core.ingest.local.AbstractLocalFileDriver;
import org.locationtech.geowave.core.ingest.local.LocalInputCommandLineOptions;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class actually executes the staging of data to HDFS based on the
 * available type plugin providers that are discovered through SPI.
 */
public class StageToHdfsDriver extends
		AbstractLocalFileDriver<AvroFormatPlugin<?, ?>, StageRunData>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(StageToHdfsDriver.class);
	private final Map<String, AvroFormatPlugin<?, ?>> ingestPlugins;
	private final String hdfsHostPort;
	private final String basePath;

	public StageToHdfsDriver(
			final Map<String, AvroFormatPlugin<?, ?>> ingestPlugins,
			final String hdfsHostPort,
			final String basePath,
			final LocalInputCommandLineOptions inputOptions ) {
		super(
				inputOptions);
		this.ingestPlugins = ingestPlugins;
		this.hdfsHostPort = hdfsHostPort;
		this.basePath = basePath;
	}

	@Override
	protected void processFile(
			final URL file,
			final String typeName,
			final AvroFormatPlugin<?, ?> plugin,
			final StageRunData runData ) {
		final DataFileWriter writer = runData.getWriter(
				typeName,
				plugin);
		if (writer != null) {
			try (final CloseableIterator<?> objs = plugin.toAvroObjects(file)) {
				while (objs.hasNext()) {
					final Object obj = objs.next();
					try {
						writer.append(obj);
					}
					catch (final IOException e) {
						LOGGER.error(
								"Cannot append data to sequence file",
								e);
					}
				}
			}
		}
	}

	public boolean runOperation(
			final String inputPath,
			final File configFile ) {

		// first collect the stage to hdfs plugins
		final Map<String, AvroFormatPlugin<?, ?>> stageToHdfsPlugins = ingestPlugins;
		final Configuration conf = new Configuration();
		conf.set(
				"fs.defaultFS",
				hdfsHostPort);
		conf.set(
				"fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		final Path hdfsBaseDirectory = new Path(
				basePath);

		try {
			try (final FileSystem fs = FileSystem.get(conf)) {
				if (!fs.exists(hdfsBaseDirectory)) {
					fs.mkdirs(hdfsBaseDirectory);
				}
				try {
					final StageRunData runData = new StageRunData(
							hdfsBaseDirectory,
							fs);
					processInput(
							inputPath,
							configFile,
							stageToHdfsPlugins,
							runData);
					runData.close();
					return true;
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unexpected I/O exception when reading input files",
							e);
					return false;
				}
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to create remote HDFS directory",
					e);
			return false;
		}
	}
}
