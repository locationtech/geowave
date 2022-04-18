/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.hdfs;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.Optional;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

public class HdfsUrlStreamHandlerFactory extends FsUrlStreamHandlerFactory {
  // The wrapped URLStreamHandlerFactory's instance
  private final Optional<URLStreamHandlerFactory> delegate;

  /** Used in case there is no existing URLStreamHandlerFactory defined */
  public HdfsUrlStreamHandlerFactory() {
    this(null);
  }

  /** Used in case there is an existing URLStreamHandlerFactory defined */
  public HdfsUrlStreamHandlerFactory(final URLStreamHandlerFactory delegate) {
    this.delegate = Optional.ofNullable(delegate);
  }

  @Override
  public URLStreamHandler createURLStreamHandler(final String protocol) {

    // FsUrlStreamHandlerFactory impl
    final URLStreamHandler urlStreamHandler = super.createURLStreamHandler(protocol);

    // See if hadoop handled it
    if (urlStreamHandler != null) {
      return urlStreamHandler;
    }

    // It is not the hdfs protocol so we delegate it to the wrapped URLStreamHandlerFactory
    return delegate.map(factory -> factory.createURLStreamHandler(protocol)).orElse(null);
  }
}
