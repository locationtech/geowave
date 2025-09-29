/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.cli;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public class GeoWaveHBaseUtility extends HBaseTestingUtility {

  public GeoWaveHBaseUtility() {
    super();
  }

  public GeoWaveHBaseUtility(Configuration conf) {
    super(conf);
  }

  @Override
  public Path getDataTestDirOnTestFS() throws IOException {
    // this is a workaround because HBase Master File System on windows causes errors if the data
    // directory doesn't have execute permissions
    Path path = super.getDataTestDirOnTestFS();
    FileSystem fs = getTestFileSystem();
    fs.mkdirs(path);

    fs.setPermission(path, FsPermission.valueOf("-rwxrwxrwx"));
    return path;
  }
}
