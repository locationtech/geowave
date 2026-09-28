/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.ingest.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geowave.core.ingest.URLIngestUtils;
import org.locationtech.geowave.core.ingest.spark.SparkIngestDriver;

/**
 * Ingest lists S3 objects as paths, hands their URIs around (to Spark executors among others), and
 * opens each one as a URL. These pin the URI form all of that depends on,
 * {@code s3://<endpoint host>/<bucket>/<key>}, and show it resolves back to the same object.
 */
public class S3UriTest {
  private static final String BUCKET = "testbucket";
  private static MockS3 mockS3;
  private static String endpoint;

  @BeforeClass
  public static void startS3() throws IOException {
    mockS3 = new MockS3();
    endpoint = mockS3.endpoint();
    mockS3.createBucket(BUCKET);
    mockS3.put(BUCKET, "dir/a.csv", "a");
    mockS3.put(BUCKET, "dir/nested/b.csv", "b");
    mockS3.put(BUCKET, "dir/with space.csv", "spaced");
  }

  @AfterClass
  public static void stopS3() throws IOException {
    mockS3.close();
  }

  /** The endpoint's port is not part of the URI; the filesystem is keyed by host alone. */
  @Test
  public void listedPathsHaveEndpointBucketKeyUris() throws IOException {
    final List<URI> uris;
    try (Stream<Path> files =
        Files.walk(URLIngestUtils.setupS3FileSystem("s3://" + BUCKET + "/dir", endpoint))) {
      uris =
          files.filter(Files::isRegularFile).map(Path::toUri).sorted().collect(Collectors.toList());
    }
    assertEquals(
        Arrays.asList(
            URI.create("s3://127.0.0.1/testbucket/dir/a.csv"),
            URI.create("s3://127.0.0.1/testbucket/dir/nested/b.csv"),
            URI.create("s3://127.0.0.1/testbucket/dir/with%20space.csv")),
        uris);
  }

  @Test
  public void uriOfAPathReadsItsObjectAsAUrl() throws IOException {
    assertEquals("a", read("dir/a.csv"));
    assertEquals("b", read("dir/nested/b.csv"));
    assertEquals("spaced", read("dir/with space.csv"));
  }

  @Test
  public void sparkExecutorsReadThroughTheFileSystemThatListed()
      throws IOException, URISyntaxException {
    final Path listed = URLIngestUtils.setupS3FileSystem("s3://" + BUCKET + "/dir", endpoint);
    assertSame(listed.getFileSystem(), new SparkIngestDriver().initializeS3FS(endpoint));
  }

  private static String read(final String key) throws IOException {
    final URI uri =
        URLIngestUtils.setupS3FileSystem("s3://" + BUCKET + "/" + key, endpoint).toUri();
    try (InputStream in = uri.toURL().openStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
