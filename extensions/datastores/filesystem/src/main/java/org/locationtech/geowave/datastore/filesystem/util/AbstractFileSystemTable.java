/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractFileSystemTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFileSystemTable.class);

  protected Path tableDirectory;
  protected final short adapterId;
  protected final String typeName;
  protected boolean visibilityEnabled;
  protected FileSystemDataFormatter formatter;

  public AbstractFileSystemTable(
      final short adapterId,
      final String typeName,
      final String format,
      final boolean visibilityEnabled) throws IOException {
    super();
    this.adapterId = adapterId;
    this.typeName = typeName;
    this.visibilityEnabled = visibilityEnabled;
    formatter = DataFormatterCache.getInstance().getFormatter(format, visibilityEnabled);
  }

  protected void setTableDirectory(final Path tableDirectory) throws IOException {
    this.tableDirectory = Files.createDirectories(tableDirectory);
  }

  public void deleteFile(final String fileName) {
    try {
      Files.delete(tableDirectory.resolve(fileName));
    } catch (final IOException e) {
      LOGGER.warn("Unable to delete file", e);
    }
  }

  protected void writeFile(final String fileName, final byte[] value) {
    try {
      Files.write(
          tableDirectory.resolve(fileName),
          value,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.SYNC);
    } catch (final IOException e) {
      LOGGER.warn("Unable to write file", e);
    }
  }
}
