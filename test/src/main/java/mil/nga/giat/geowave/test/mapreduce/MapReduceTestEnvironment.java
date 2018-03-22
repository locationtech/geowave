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
package mil.nga.giat.geowave.test.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.mapreduce.operations.ConfigHDFSCommand;
import mil.nga.giat.geowave.test.TestEnvironment;
import mil.nga.giat.geowave.test.TestUtils;

public class MapReduceTestEnvironment implements
		TestEnvironment
{
	private final static Logger LOGGER = LoggerFactory.getLogger(MapReduceTestEnvironment.class);

	private static MapReduceTestEnvironment singletonInstance = null;

	public static synchronized MapReduceTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new MapReduceTestEnvironment();
		}
		return singletonInstance;
	}

	protected static final String HDFS_BASE_DIRECTORY = "test_tmp";
	private static final String DEFAULT_JOB_TRACKER = "local";
	private String jobtracker;
	private String hdfs;
	private boolean hdfsProtocol;
	private String hdfsBaseDirectory;
	private ManualOperationParams operationParams;
	private File configFile;

	private MapReduceTestEnvironment() {}

	@Override
	public void setup()
			throws Exception {
		hdfs = System.getProperty("hdfs");
		jobtracker = System.getProperty("jobtracker");
		if (!TestUtils.isSet(hdfs)) {
			hdfs = "file:///";

			hdfsBaseDirectory = TestUtils.TEMP_DIR.toURI().toURL().toString() + "/" + HDFS_BASE_DIRECTORY;
			hdfsProtocol = false;
			// create temporary config file and use it for hdfs FS URL config
			configFile = File.createTempFile(
					"test_mr",
					null);
			operationParams = new ManualOperationParams();
			operationParams.getContext().put(
					ConfigOptions.PROPERTIES_FILE_CONTEXT,
					configFile);

			final ConfigHDFSCommand configHdfs = new ConfigHDFSCommand();
			configHdfs.setHdfsUrlParameter(hdfs);
			configHdfs.execute(operationParams);
		}
		else {
			hdfsBaseDirectory = HDFS_BASE_DIRECTORY;
			if (!hdfs.contains("://")) {
				hdfs = "hdfs://" + hdfs;
				hdfsProtocol = true;
			}
			else {
				hdfsProtocol = hdfs.toLowerCase(
						Locale.ENGLISH).startsWith(
						"hdfs://");
			}
		}
		if (!TestUtils.isSet(jobtracker)) {
			jobtracker = DEFAULT_JOB_TRACKER;
		}
	}

	@Override
	public void tearDown() {
		try {
			if (hdfsProtocol) {
				final Path tmpDir = new Path(
						hdfsBaseDirectory);
				FileSystem fs = null;
				try {
					fs = FileSystem.get(MapReduceTestUtils.getConfiguration());
					fs.delete(
							tmpDir,
							true);
				}
				finally {
					if (fs != null) fs.close();
				}
				if (configFile != null && configFile.exists() && configFile.delete()) {
					configFile = null;
				}
			}
			else {
				FileUtils.deleteDirectory(new File(
						hdfsBaseDirectory.replace(
								"file:",
								"").replace(
								"/C:",
								"")));
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to delete HDFS temp directory",
					e);
		}
	}

	public String getJobtracker() {
		return jobtracker;
	}

	public void setJobtracker(
			String jobtracker ) {
		this.jobtracker = jobtracker;
	}

	public String getHdfs() {
		return hdfs;
	}

	public void setHdfs(
			String hdfs ) {
		this.hdfs = hdfs;
	}

	public boolean isHdfsProtocol() {
		return hdfsProtocol;
	}

	public void setHdfsProtocol(
			boolean hdfsProtocol ) {
		this.hdfsProtocol = hdfsProtocol;
	}

	public String getHdfsBaseDirectory() {
		return hdfsBaseDirectory;
	}

	public void setHdfsBaseDirectory(
			String hdfsBaseDirectory ) {
		this.hdfsBaseDirectory = hdfsBaseDirectory;
	}

	public ManualOperationParams getOperationParams() {
		return operationParams;
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}

}
