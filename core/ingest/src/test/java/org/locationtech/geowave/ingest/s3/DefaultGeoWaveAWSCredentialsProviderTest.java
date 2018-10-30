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
package org.locationtech.geowave.ingest.s3;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.ingest.IngestUtils;
import org.locationtech.geowave.core.ingest.IngestUtils.URLTYPE;
import org.locationtech.geowave.core.ingest.spark.SparkIngestDriver;

import com.upplication.s3fs.S3FileSystem;

import io.findify.s3mock.S3Mock;

public class DefaultGeoWaveAWSCredentialsProviderTest
{

	@Test
	public void testAnonymousAccess()
			throws NoSuchFieldException,
			SecurityException,
			IllegalArgumentException,
			IllegalAccessException,
			URISyntaxException,
			IOException {
		File temp = File.createTempFile(
				"temp",
				Long.toString(System.nanoTime()));
		temp.mkdirs();
		S3Mock mockS3 = new S3Mock.Builder().withPort(
				8001).withFileBackend(
				temp.getAbsolutePath()).withInMemoryBackend().build();
		mockS3.start();
		IngestUtils.setURLStreamHandlerFactory(URLTYPE.S3);
		SparkIngestDriver sparkDriver = new SparkIngestDriver();
		S3FileSystem s3 = sparkDriver.initializeS3FS("s3://s3.amazonaws.com");
		s3.getClient().setEndpoint(
				"http://127.0.0.1:8001");
		s3.getClient().createBucket(
				"testbucket");
		s3.getClient().putObject(
				"testbucket",
				"test",
				"content");
		try (Stream<Path> s = Files.list(IngestUtils.setupS3FileSystem(
				"s3://testbucket/",
				"s3://s3.amazonaws.com"))) {
			Assert.assertEquals(
					1,
					s.count());
		}
		mockS3.shutdown();
	}
}
