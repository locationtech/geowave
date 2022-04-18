/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;
import org.locationtech.geowave.core.store.ingest.IngestOptionsBuilderImpl;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;

/**
 * When ingesting into a DataStore from a URL, this is a set of available options that can be
 * provided. Use the Builder to construct IngestOptions.
 *
 * @param <T> the type for entries that are being ingested
 */
public class IngestOptions<T> {
  /**
   * A Builder to create IngestOptions
   *
   * @param <T> the type for entries that are being ingested
   */
  public static interface Builder<T> {
    /**
     * the ingest format plugin which does the actual parsing of the files and converting to GeoWave
     * entries
     *
     * @param format the format
     * @return this builder
     */
    Builder<T> format(LocalFileIngestPlugin<T> format);

    /**
     * Number of threads to use for ingest
     *
     * @param threads the number of threads
     * @return this builder
     */
    Builder<T> threads(int threads);

    /**
     * Set a visibility handler that will be applied to all data ingested
     *
     * @param visibilityHandler the visibility handler to use
     * @return this builder
     */
    Builder<T> visibility(VisibilityHandler visibilityHandler);

    /**
     * Set an array of acceptable file extensions. If this is empty, all files will be accepted
     * regardless of extension. Additionally each format plugin may only accept certain file
     * extensions.
     *
     * @param fileExtensions the array of acceptable file extensions
     * @return this builder
     */
    Builder<T> extensions(String[] fileExtensions);

    /**
     * Add a new file extension to the array of acceptable file extensions
     *
     * @param fileExtension the file extension to add
     * @return this builder
     */
    Builder<T> addExtension(String fileExtension);

    /**
     * Filter data prior to being ingesting using a Predicate (if transform is provided, transform
     * will be applied before the filter)
     *
     * @param filter the filter
     * @return this builder
     */
    Builder<T> filter(Predicate<T> filter);

    /**
     * Transform the data prior to ingestion
     *
     * @param transform the transform function
     * @return this builder
     */
    Builder<T> transform(Function<T, T> transform);

    /**
     * register a callback to get notifications of the data and its insertion ID(s) within the
     * indices after it has been ingested.
     *
     * @param callback the callback
     * @return this builder
     */
    Builder<T> callback(IngestCallback<T> callback);

    /**
     * provide properties used for particular URL handlers
     *
     * @param properties for URL handlers such as s3.endpoint.url=s3.amazonaws.com or
     *        hdfs.defaultFS.url=sandbox.mydomain.com:8020
     * @return this builder
     */
    Builder<T> properties(Properties properties);

    /**
     * Construct the IngestOptions with the provided values from this builder
     *
     * @return the IngestOptions
     */
    IngestOptions<T> build();
  }

  /**
   * get a default implementation of this builder
   *
   * @return a new builder
   */
  public static <T> Builder<T> newBuilder() {
    return new IngestOptionsBuilderImpl<T>();
  }

  /**
   * An interface to get callbacks of ingest
   *
   * @param <T> the type of data ingested
   */
  public static interface IngestCallback<T> {
    void dataWritten(WriteResults insertionIds, T data);
  }

  private final LocalFileIngestPlugin<T> format;
  private final int threads;
  private final VisibilityHandler visibilityHandler;
  private final String[] fileExtensions;
  private final Predicate<T> filter;
  private final Function<T, T> transform;
  private final IngestCallback<T> callback;
  private final Properties properties;

  /**
   * Use the Builder to construct instead of this constructor.
   *
   * @param format the ingest format plugin
   * @param threads number of threads
   * @param globalVisibility visibility applied to all entries
   * @param fileExtensions an array of acceptable file extensions
   * @param filter a function to filter entries prior to ingest
   * @param transform a function to transform entries prior to ingest
   * @param callback a callback to get entries ingested and their insertion ID(s) in GeoWave
   * @param properties properties used for particular URL handlers
   */
  public IngestOptions(
      final LocalFileIngestPlugin<T> format,
      final int threads,
      final VisibilityHandler visibilityHandler,
      final String[] fileExtensions,
      final Predicate<T> filter,
      final Function<T, T> transform,
      final IngestCallback<T> callback,
      final Properties properties) {
    super();
    this.format = format;
    this.threads = threads;
    this.visibilityHandler = visibilityHandler;
    this.fileExtensions = fileExtensions;
    this.filter = filter;
    this.transform = transform;
    this.callback = callback;
    this.properties = properties;
  }

  public LocalFileIngestPlugin<T> getFormat() {
    return format;
  }

  public int getThreads() {
    return threads;
  }

  public VisibilityHandler getVisibilityHandler() {
    return visibilityHandler;
  }

  public String[] getFileExtensions() {
    return fileExtensions;
  }

  public Predicate<T> getFilter() {
    return filter;
  }

  public Function<T, T> getTransform() {
    return transform;
  }

  public IngestCallback<T> getCallback() {
    return callback;
  }

  public Properties getProperties() {
    return properties;
  }
}
