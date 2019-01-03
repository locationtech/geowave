/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.config;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.rocksdb.RocksDBStoreFactoryFamily;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBUtils;

public class RocksDBOptions extends StoreFactoryOptions {
  @Parameter(
      names = "--directory",
      description =
          "The directory to read/write to.  Defaults to \"rocksdb\" in the working directory.")
  private String directory = "rocksdb";

  @ParametersDelegate
  protected BaseDataStoreOptions baseOptions =
      new BaseDataStoreOptions() {
        @Override
        public boolean isServerSideLibraryEnabled() {
          return false;
        }

        @Override
        protected int defaultMaxRangeDecomposition() {
          return RocksDBUtils.ROCKSDB_DEFAULT_MAX_RANGE_DECOMPOSITION;
        }

        @Override
        protected int defaultAggregationMaxRangeDecomposition() {
          return RocksDBUtils.ROCKSDB_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION;
        }
      };

  public RocksDBOptions() {
    super();
  }

  public RocksDBOptions(final String geowaveNamespace) {
    super(geowaveNamespace);
  }

  public void setDirectory(final String directory) {
    this.directory = directory;
  }

  public String getDirectory() {
    return directory;
  }

  @Override
  public StoreFactoryFamilySpi getStoreFactory() {
    return new RocksDBStoreFactoryFamily();
  }

  @Override
  public DataStoreOptions getStoreOptions() {
    return baseOptions;
  }
}
