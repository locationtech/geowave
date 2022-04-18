/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.ingest;

import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.store.api.IngestOptions;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.api.IngestOptions.Builder;
import org.locationtech.geowave.core.store.api.IngestOptions.IngestCallback;

public class IngestOptionsBuilderImpl<T> implements Builder<T> {

  private LocalFileIngestPlugin<T> format = null;
  private int threads = 1;
  private VisibilityHandler visibilityHandler = null;
  private String[] fileExtensions = new String[0];
  private Predicate<T> filter = null;
  private Function<T, T> transform = null;
  private IngestCallback<T> callback = null;
  private Properties properties = null;

  @Override
  public Builder<T> format(final LocalFileIngestPlugin<T> format) {
    this.format = format;
    return this;
  }

  @Override
  public Builder<T> threads(final int threads) {
    this.threads = threads;
    return this;
  }

  @Override
  public Builder<T> visibility(final VisibilityHandler visibilityHandler) {
    this.visibilityHandler = visibilityHandler;
    return this;
  }

  @Override
  public Builder<T> extensions(final String[] fileExtensions) {
    this.fileExtensions = fileExtensions;
    return this;
  }

  @Override
  public Builder<T> addExtension(final String fileExtension) {
    fileExtensions = ArrayUtils.add(fileExtensions, fileExtension);
    return this;
  }

  @Override
  public Builder<T> filter(final Predicate<T> filter) {
    this.filter = filter;
    return this;
  }

  @Override
  public Builder<T> transform(final Function<T, T> transform) {
    this.transform = transform;
    return this;
  }

  @Override
  public Builder<T> callback(final IngestCallback<T> callback) {
    this.callback = callback;
    return this;
  }

  @Override
  public Builder<T> properties(final Properties properties) {
    this.properties = properties;
    return this;
  }

  @Override
  public IngestOptions<T> build() {
    return new IngestOptions<>(
        format,
        threads,
        visibilityHandler,
        fileExtensions,
        filter,
        transform,
        callback,
        properties);
  }
}
