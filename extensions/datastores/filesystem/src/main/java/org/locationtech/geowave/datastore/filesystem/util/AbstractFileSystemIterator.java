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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.primitives.UnsignedBytes;

public abstract class AbstractFileSystemIterator<T> implements CloseableIterator<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFileSystemIterator.class);
  // this is a memoized supplier to support lazy evaluation because readRow actually relies on
  // member variables set in child constructors
  final Iterator<Pair<FileSystemKey, Path>> iterator;
  boolean closed = false;

  public AbstractFileSystemIterator(
      final Path subDirectory,
      final byte[] startKey,
      final byte[] endKey,
      final Function<String, FileSystemKey> fileNameToKey) {
    this(subDirectory, startKey, endKey, false, fileNameToKey);
  }

  public AbstractFileSystemIterator(
      final Path subDirectory,
      final byte[] startKey,
      final byte[] endKey,
      final boolean endKeyInclusive,
      final Function<String, FileSystemKey> fileNameToKey) {
    super();
    iterator =
        FileSystemUtils.getSortedSet(
            subDirectory,
            startKey,
            endKey,
            endKeyInclusive,
            fileNameToKey).iterator();
  }

  public AbstractFileSystemIterator(
      final Path subDirectory,
      final Collection<ByteArrayRange> ranges,
      final Function<String, FileSystemKey> fileNameToKey) {
    super();
    iterator =
        FileSystemUtils.getSortedSet(subDirectory, fileNameToKey).stream().filter(
            p -> inRanges(ranges, p.getKey().getSortOrderKey())).iterator();
  }

  private static boolean inRanges(final Collection<ByteArrayRange> ranges, final byte[] key) {
    if ((ranges == null) || ranges.isEmpty()) {
      return true;
    }
    for (final ByteArrayRange range : ranges) {
      if (inRange(range, key)) {
        return true;
      }
    }
    return false;
  }

  private static boolean inRange(final ByteArrayRange range, final byte[] key) {

    return ((range.getStart() == null)
        || (UnsignedBytes.lexicographicalComparator().compare(range.getStart(), key) <= 0))
        && ((range.getEnd() == null)
            || (UnsignedBytes.lexicographicalComparator().compare(
                range.getEndAsNextPrefix(),
                key) > 0));
  }

  @Override
  public boolean hasNext() {
    return !closed && iterator.hasNext();
  }

  @Override
  public T next() {
    if (closed) {
      throw new NoSuchElementException();
    }
    Pair<FileSystemKey, Path> next = iterator.next();
    while (!Files.exists(next.getRight())) {
      if (!iterator.hasNext()) {
        LOGGER.warn("No more files exist in the directory");
        return null;
      }
      next = iterator.next();
    }
    try {
      return readRow(next.getLeft(), Files.readAllBytes(next.getRight()));
    } catch (final IOException e) {
      LOGGER.warn("Unable to read file " + next, e);
    }

    return null;
  }

  protected abstract T readRow(FileSystemKey key, byte[] value);

  @Override
  public void close() {
    closed = true;
  }
}
