/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem;

import org.locationtech.geowave.core.store.BaseDataStoreFamily;
import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.api.DataStore;

public class FileSystemStoreFactoryFamily extends BaseDataStoreFamily {
  private static final String TYPE = "filesystem";
  private static final String DESCRIPTION =
      "A GeoWave store backed by data in a Java NIO FileSystem (can be S3, HDFS, or a traditional file system). This can serve a purpose, but under most circumstances rocksdb would be recommended for performance reasons.";

  public FileSystemStoreFactoryFamily() {
    super(TYPE, DESCRIPTION, new FileSystemFactoryHelper());
  }

  @Override
  public GenericStoreFactory<DataStore> getDataStoreFactory() {
    return new FileSystemDataStoreFactory(TYPE, DESCRIPTION, new FileSystemFactoryHelper());
  }
}
