/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce;

import java.net.URLStreamHandler;
import java.net.spi.URLStreamHandlerProvider;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.locationtech.geowave.mapreduce.s3.S3URLStreamHandler;

/**
 * Teaches {@link java.net.URL} to open {@code s3://} and {@code hdfs://}.
 *
 * <p> This replaces several copies of a routine that called {@code URL.setURLStreamHandlerFactory}
 * and, because the JDK permits that exactly once per JVM, reflected into {@code URL.factory} and
 * {@code URL.streamHandlerLock} to install a second handler behind the first.
 * {@code streamHandlerLock} was removed in JDK 18, so no amount of {@code --add-opens} brings that
 * approach back. The service-provider interface is the supported replacement and needs no chaining:
 * the JDK asks every registered provider in turn.
 *
 * <p> Only the two protocols GeoWave means to add are answered. Returning a handler for anything
 * else would hand the rest of the JVM's URLs to Hadoop's filesystem layer, which has entries for
 * schemes such as {@code http} -- the previous factory delegated everything to Hadoop and had that
 * exposure.
 *
 * <p> The catch is where this can be seen from: the JDK loads providers with
 * {@code ServiceLoader.load(URLStreamHandlerProvider.class, ClassLoader.getSystemClassLoader())},
 * so GeoWave has to be on the system classpath for this to take effect. Code running under a
 * child-first loader -- an Accumulo iterator, a coprocessor -- has to keep installing a factory
 * directly, which is why {@code QueryFilterIterator} still does.
 *
 * <p> Nothing here may log or hold a logger. The JDK constructs providers from inside
 * {@code URL.<init>}, so anything that fails during class initialisation surfaces as a
 * {@code ServiceConfigurationError} out of every {@code new URL(..)} in the JVM, whatever the
 * protocol. An slf4j binding that cannot initialise is enough to do it.
 */
public class GeoWaveURLStreamHandlerProvider extends URLStreamHandlerProvider {
  private static final String S3_PROTOCOL = "s3";
  private static final String HDFS_PROTOCOL = "hdfs";

  private volatile FsUrlStreamHandlerFactory hadoopFactory;

  @Override
  public URLStreamHandler createURLStreamHandler(final String protocol) {
    if (S3_PROTOCOL.equals(protocol)) {
      return new S3URLStreamHandler();
    }
    if (HDFS_PROTOCOL.equals(protocol)) {
      return hadoopHandler(protocol);
    }
    return null;
  }

  private URLStreamHandler hadoopHandler(final String protocol) {
    // Constructing this reads Hadoop's configuration, so it is deferred until something asks for
    // an hdfs URL rather than run while the JVM is resolving its first URL of any kind. A failure
    // is left to propagate: the caller asked for hdfs and hdfs is not usable, and there is nowhere
    // safe to report it from here.
    FsUrlStreamHandlerFactory factory = hadoopFactory;
    if (factory == null) {
      synchronized (this) {
        factory = hadoopFactory;
        if (factory == null) {
          factory = new FsUrlStreamHandlerFactory();
          hadoopFactory = factory;
        }
      }
    }
    return factory.createURLStreamHandler(protocol);
  }
}
