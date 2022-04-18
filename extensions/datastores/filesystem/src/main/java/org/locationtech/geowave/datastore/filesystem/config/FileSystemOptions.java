/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.config;

import java.nio.file.Paths;
import java.util.Properties;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.filesystem.FileSystemStoreFactoryFamily;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemUtils;
import org.locationtech.geowave.datastore.filesystem.util.GeoWaveBinaryDataFormatter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.internal.Console;

public class FileSystemOptions extends StoreFactoryOptions {
  @Parameter(
      names = "--dir",
      description = "The directory to read/write to.  Defaults to \"geowave\" in the working directory.")
  private String dir = "geowave";

  @Parameter(
      names = "--format",
      description = "Optionally uses a formatter plugin.  Defaults to \""
          + GeoWaveBinaryDataFormatter.DEFAULT_BINARY_FORMATTER
          + "\" which is a compact geowave serialization.  Use `geowave util filesystem listformats` to see available formats.")
  private String format = "binary";

  @ParametersDelegate
  protected BaseDataStoreOptions baseOptions = new BaseDataStoreOptions() {
    @Override
    public boolean isServerSideLibraryEnabled() {
      return false;
    }

    @Override
    protected int defaultMaxRangeDecomposition() {
      return FileSystemUtils.FILESYSTEM_DEFAULT_MAX_RANGE_DECOMPOSITION;
    }

    @Override
    protected int defaultAggregationMaxRangeDecomposition() {
      return FileSystemUtils.FILESYSTEM_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION;
    }

    @Override
    protected boolean defaultEnableVisibility() {
      return false;
    }
  };

  public String getFormat() {
    return format;
  }

  public void setFormat(final String format) {
    this.format = format;
  }

  @Override
  public void validatePluginOptions(final Console console) throws ParameterException {
    // Set the directory to be absolute
    dir = Paths.get(dir).toAbsolutePath().toString();
    super.validatePluginOptions(console);
  }

  @Override
  public void validatePluginOptions(final Properties properties, final Console console)
      throws ParameterException {
    // Set the directory to be absolute
    dir = Paths.get(dir).toAbsolutePath().toString();
    super.validatePluginOptions(properties, console);
  }

  public FileSystemOptions() {
    super();
  }

  public FileSystemOptions(final String geowaveNamespace) {
    super(geowaveNamespace);
  }

  public void setDirectory(final String dir) {
    this.dir = dir;
  }

  public String getDirectory() {
    return dir;
  }

  @Override
  public StoreFactoryFamilySpi getStoreFactory() {
    return new FileSystemStoreFactoryFamily();
  }

  @Override
  public DataStoreOptions getStoreOptions() {
    return baseOptions;
  }
}
