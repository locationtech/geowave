/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.RangeReaderParams;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.IndexFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;

public class FileSystemUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemUtils.class);

  public static int FILESYSTEM_DEFAULT_MAX_RANGE_DECOMPOSITION = 250;
  public static int FILESYSTEM_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION = 250;

  public static SortedSet<Pair<FileSystemKey, Path>> getSortedSet(
      final Path subDirectory,
      final Function<String, FileSystemKey> fileNameToKey) {
    return getSortedSet(subDirectory, null, null, false, fileNameToKey);
  }

  public static SortedSet<Pair<FileSystemKey, Path>> getSortedSet(
      final Path subDirectory,
      final byte[] startKeyInclusive,
      final byte[] endKey,
      final boolean endKeyInclusive,
      final Function<String, FileSystemKey> fileNameToKey) {
    try {
      final Supplier<NavigableSet<Pair<FileSystemKey, Path>>> sortedSetFactory =
          () -> new TreeSet<>();
      NavigableSet<Pair<FileSystemKey, Path>> sortedSet =
          Files.walk(subDirectory).filter(Files::isRegularFile).map(
              path -> Pair.of(fileNameToKey.apply(path.getFileName().toString()), path)).collect(
                  Collectors.toCollection(sortedSetFactory));
      if (startKeyInclusive != null) {
        sortedSet =
            sortedSet.tailSet(
                Pair.of(
                    new BasicFileSystemKey(startKeyInclusive),
                    subDirectory.resolve(keyToFileName(startKeyInclusive))),
                true);
      }
      if (endKey != null) {
        sortedSet =
            sortedSet.headSet(
                Pair.of(
                    new BasicFileSystemKey(endKey),
                    subDirectory.resolve(keyToFileName(endKey))),
                endKeyInclusive);
      }
      return sortedSet;
    } catch (final IOException e) {
      LOGGER.warn("Unable to iterate through file system", e);
    }
    return new TreeSet<>();
  }

  public static Path getSubdirectory(
      final String parentDir,
      final String subdirectory1,
      final String subdirectory2) {
    if ((subdirectory1 != null) && !subdirectory1.trim().isEmpty()) {
      if ((subdirectory2 != null) && !subdirectory2.trim().isEmpty()) {
        return Paths.get(parentDir, subdirectory1, subdirectory2);
      } else {
        return Paths.get(parentDir, subdirectory1);
      }
    } else if ((subdirectory2 != null) && !subdirectory2.trim().isEmpty()) {
      return Paths.get(parentDir, subdirectory2);
    } else {
      return Paths.get(parentDir);
    }
  }

  public static Path getSubdirectory(final String parentDir, final String subdirectory) {
    if ((subdirectory != null) && !subdirectory.trim().isEmpty()) {
      return Paths.get(parentDir, subdirectory);
    } else {
      return Paths.get(parentDir);
    }
  }

  public static void visit(
      final Path subDirectory,
      final byte[] startKeyInclusive,
      final byte[] endKeyExclusive,
      final Consumer<Path> pathVisitor,
      final Function<String, FileSystemKey> fileNameToKey) {
    getSortedSet(
        subDirectory,
        startKeyInclusive,
        endKeyExclusive,
        false,
        fileNameToKey).stream().map(Pair::getRight).forEach(pathVisitor);
  }

  public static FileSystemDataIndexTable getDataIndexTable(
      final FileSystemClient client,
      final short adapterId,
      final String typeName) {
    return client.getDataIndexTable(adapterId, typeName);
  }

  public static Path getMetadataTablePath(
      final String subDirectory,
      final String format,
      final boolean visibilityEnabled,
      final MetadataType type) {
    final String metadataDirectory =
        DataFormatterCache.getInstance().getFormatter(
            format,
            visibilityEnabled).getMetadataDirectory();
    return getSubdirectory(subDirectory, metadataDirectory, type.id());
  }

  public static FileSystemIndexTable getIndexTable(
      final FileSystemClient client,
      final short adapterId,
      final String typeName,
      final String indexName,
      final byte[] partitionKey,
      final boolean requiresTimestamp) {
    return client.getIndexTable(adapterId, typeName, indexName, partitionKey, requiresTimestamp);
  }

  public static boolean isSortByTime(final InternalDataAdapter<?> adapter) {
    return adapter.getAdapter() instanceof RowMergingDataAdapter;
  }

  public static boolean isSortByKeyRequired(final RangeReaderParams<?> params) {
    // subsampling needs to be sorted by sort key to work properly
    return (params.getMaxResolutionSubsamplingPerDimension() != null)
        && (params.getMaxResolutionSubsamplingPerDimension().length > 0);
  }

  public static Pair<Boolean, Boolean> isGroupByRowAndIsSortByTime(
      final RangeReaderParams<?> readerParams,
      final short adapterId) {
    final boolean sortByTime = isSortByTime(readerParams.getAdapterStore().getAdapter(adapterId));
    return Pair.of(readerParams.isMixedVisibility() || sortByTime, sortByTime);
  }

  public static Iterator<GeoWaveRow> sortBySortKey(final Iterator<GeoWaveRow> it) {
    return Streams.stream(it).sorted(SortKeyOrder.SINGLETON).iterator();
  }

  public static FileSystemMetadataTable getMetadataTable(
      final FileSystemClient client,
      final MetadataType metadataType) {
    return client.getMetadataTable(metadataType);
  }

  public static Set<ByteArray> getPartitions(
      final Path directory,
      final IndexFormatter indexFormatter,
      final String indexName,
      final String typeName) {
    return recurseDirectoriesToString(
        directory,
        "",
        new HashSet<>(),
        indexFormatter,
        indexName,
        typeName);
  }

  private static Set<ByteArray> recurseDirectoriesToString(
      final Path currentPath,
      final String subdirectoryName,
      final Set<ByteArray> partitionDirectories,
      final IndexFormatter indexFormatter,
      final String indexName,
      final String typeName) {
    try {
      final AtomicBoolean atLeastOneRegularFile = new AtomicBoolean(false);
      Files.list(currentPath).filter(p -> {
        if (Files.isDirectory(p)) {
          return true;
        } else {
          atLeastOneRegularFile.set(true);
          return false;
        }
      }).forEach(
          path -> recurseDirectoriesToString(
              path,
              (subdirectoryName == null) || subdirectoryName.isEmpty()
                  ? path.getFileName().toString()
                  : subdirectoryName + "/" + path.getFileName().toString(),
              partitionDirectories,
              indexFormatter,
              indexName,
              typeName));
      if (atLeastOneRegularFile.get()) {
        partitionDirectories.add(
            new ByteArray(indexFormatter.getPartitionKey(indexName, typeName, subdirectoryName)));
      }
    } catch (final IOException e) {
      LOGGER.warn("Cannot list files in " + subdirectoryName, e);
    }
    return partitionDirectories;
  }

  private static class SortKeyOrder implements Comparator<GeoWaveRow>, Serializable {
    private static SortKeyOrder SINGLETON = new SortKeyOrder();
    private static final long serialVersionUID = 23275155231L;

    @Override
    public int compare(final GeoWaveRow o1, final GeoWaveRow o2) {
      if (o1 == o2) {
        return 0;
      }
      if (o1 == null) {
        return 1;
      }
      if (o2 == null) {
        return -1;
      }
      byte[] otherComp = o2.getSortKey() == null ? new byte[0] : o2.getSortKey();
      byte[] thisComp = o1.getSortKey() == null ? new byte[0] : o1.getSortKey();

      int comp = UnsignedBytes.lexicographicalComparator().compare(thisComp, otherComp);
      if (comp != 0) {
        return comp;
      }
      otherComp = o2.getPartitionKey() == null ? new byte[0] : o2.getPartitionKey();
      thisComp = o1.getPartitionKey() == null ? new byte[0] : o1.getPartitionKey();

      comp = UnsignedBytes.lexicographicalComparator().compare(thisComp, otherComp);
      if (comp != 0) {
        return comp;
      }
      comp = Short.compare(o1.getAdapterId(), o2.getAdapterId());
      if (comp != 0) {
        return comp;
      }
      otherComp = o2.getDataId() == null ? new byte[0] : o2.getDataId();
      thisComp = o1.getDataId() == null ? new byte[0] : o1.getDataId();

      comp = UnsignedBytes.lexicographicalComparator().compare(thisComp, otherComp);

      if (comp != 0) {
        return comp;
      }
      return Integer.compare(o1.getNumberOfDuplicates(), o2.getNumberOfDuplicates());
    }
  }

  protected static String keyToFileName(final byte[] key) {
    return ByteArrayUtils.byteArrayToString(key) + ".bin";
  }

  protected static byte[] fileNameToKey(final String key) {
    if (key.length() < 5) {
      return new byte[0];
    }
    return ByteArrayUtils.byteArrayFromString(key.substring(0, key.length() - 4));
  }
}
