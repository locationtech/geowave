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
import java.util.Arrays;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.DataIndexFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemDataIndexTable extends AbstractFileSystemTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemDataIndexTable.class);

  public FileSystemDataIndexTable(
      final String subDirectory,
      final short adapterId,
      final String typeName,
      final String format,
      final boolean visibilityEnabled) throws IOException {
    super(adapterId, typeName, format, visibilityEnabled);
    setTableDirectory(
        FileSystemUtils.getSubdirectory(
            subDirectory,
            formatter.getDataIndexFormatter().getDirectoryName(typeName)));
  }

  public synchronized void add(final byte[] dataId, final GeoWaveValue value) {
    writeFile(
        formatter.getDataIndexFormatter().getFileName(typeName, dataId),
        formatter.getDataIndexFormatter().getFileContents(typeName, dataId, value));
  }

  public CloseableIterator<GeoWaveRow> dataIndexIterator(final byte[][] dataIds) {
    final DataIndexFormatter dataIndexFormatter = formatter.getDataIndexFormatter();
    return new CloseableIterator.Wrapper(
        Arrays.stream(dataIds).map(
            // convert to pair with path so the path is only instantiated once (depending on
            // filesystem (such as S3 or HDFS) can be modestly expensive
            dataId -> Pair.of(
                dataId,
                tableDirectory.resolve(dataIndexFormatter.getFileName(typeName, dataId)))).filter(
                    p -> Files.exists(p.getRight())).map(pair -> {
                      try {
                        return new GeoWaveRowImpl(
                            new GeoWaveKeyImpl(
                                pair.getLeft(),
                                adapterId,
                                new byte[0],
                                new byte[0],
                                0),
                            new GeoWaveValue[] {
                                dataIndexFormatter.getValue(
                                    pair.getRight().getFileName().toString(),
                                    typeName,
                                    pair.getLeft(),
                                    Files.readAllBytes(pair.getRight()))});
                      } catch (final IOException e) {
                        LOGGER.error(
                            "Unable to read value by data ID for file '" + pair.getRight() + "'",
                            e);
                        return null;
                      }
                    }).filter(Objects::nonNull).iterator());
  }

  public CloseableIterator<GeoWaveRow> dataIndexIterator(
      final byte[] startDataId,
      final byte[] endDataId) {
    return new DataIndexRowIterator(
        tableDirectory,
        startDataId,
        endDataId,
        adapterId,
        typeName,
        formatter.getDataIndexFormatter());
  }

  public void deleteDataId(final byte[] dataId) {
    deleteFile(formatter.getDataIndexFormatter().getFileName(typeName, dataId));
  }
}
