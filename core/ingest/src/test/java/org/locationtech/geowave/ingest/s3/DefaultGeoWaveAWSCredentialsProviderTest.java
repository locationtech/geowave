/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.ingest.s3;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.ingest.URLIngestUtils;
import org.locationtech.geowave.core.ingest.URLIngestUtils.URLTYPE;
import org.locationtech.geowave.core.ingest.spark.SparkIngestDriver;
import com.upplication.s3fs.S3FileSystem;
import io.findify.s3mock.S3Mock;

public class DefaultGeoWaveAWSCredentialsProviderTest {

  @Test
  public void testAnonymousAccess() throws NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException, URISyntaxException, IOException {
    final File temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
    temp.mkdirs();
    final S3Mock mockS3 =
        new S3Mock.Builder().withPort(8001).withFileBackend(
            temp.getAbsolutePath()).withInMemoryBackend().build();
    mockS3.start();
    URLIngestUtils.setURLStreamHandlerFactory(URLTYPE.S3);
    final SparkIngestDriver sparkDriver = new SparkIngestDriver();
    final S3FileSystem s3 = sparkDriver.initializeS3FS("s3://s3.amazonaws.com");
    s3.getClient().setEndpoint("http://127.0.0.1:8001");
    s3.getClient().createBucket("testbucket");
    s3.getClient().putObject("testbucket", "test", "content");
    try (Stream<Path> s =
        Files.list(URLIngestUtils.setupS3FileSystem("s3://testbucket/", "s3://s3.amazonaws.com"))) {
      Assert.assertEquals(1, s.count());
    }
    mockS3.shutdown();
  }
}
