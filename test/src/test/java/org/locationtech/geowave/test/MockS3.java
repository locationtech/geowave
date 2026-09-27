/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.FileSystemNotFoundException;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import io.findify.s3mock.S3Mock;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * An in-memory S3 endpoint on a free local port, standing in for a bucket that ITs would otherwise
 * need credentials and network access for. It only speaks HTTP, so {@code s3fs.protocol} is
 * {@code http} while it is open. S3 filesystems are shared per host, not per port, so closing it
 * also closes the one GeoWave opened on it, leaving the next mock a fresh one.
 */
public class MockS3 implements AutoCloseable {
  private static final String PROTOCOL_PROPERTY = "s3fs.protocol";
  private final String previousProtocol;
  private final S3Mock server;
  private final S3Client client;
  private final int port;

  public MockS3() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    server = new S3Mock.Builder().withPort(port).withInMemoryBackend().build();
    server.start();
    client =
        S3Client.builder().endpointOverride(URI.create("http://127.0.0.1:" + port)).region(
            Region.US_EAST_1).credentialsProvider(
                AnonymousCredentialsProvider.create()).forcePathStyle(true)
            // s3mock predates the SDK's default request checksums and would store their framing
            .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED).build();
    previousProtocol = System.setProperty(PROTOCOL_PROPERTY, "http");
  }

  /** The endpoint as {@code geowave config aws} would be given it. */
  public String endpoint() {
    return "127.0.0.1:" + port;
  }

  public void put(final String bucket, final String key, final File file) {
    if (client.listBuckets().buckets().stream().noneMatch(b -> b.name().equals(bucket))) {
      client.createBucket(b -> b.bucket(bucket));
    }
    client.putObject(b -> b.bucket(bucket).key(key), RequestBody.fromFile(file));
  }

  @Override
  public void close() throws IOException {
    try {
      new S3FileSystemProvider().getFileSystem(URI.create("s3://" + endpoint())).close();
    } catch (final FileSystemNotFoundException e) {
      // nothing was read from it
    }
    if (previousProtocol == null) {
      System.clearProperty(PROTOCOL_PROPERTY);
    } else {
      System.setProperty(PROTOCOL_PROPERTY, previousProtocol);
    }
    client.close();
    server.shutdown();
  }
}
