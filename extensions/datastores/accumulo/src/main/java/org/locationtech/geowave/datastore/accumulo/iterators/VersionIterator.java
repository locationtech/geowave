/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.locationtech.geowave.core.cli.VersionUtils;
import org.locationtech.geowave.core.index.StringUtils;

public class VersionIterator implements SortedKeyValueIterator<Key, Value> {
  private boolean done = false;

  @Override
  public void init(
      final SortedKeyValueIterator<Key, Value> source,
      final Map<String, String> options,
      final IteratorEnvironment env) throws IOException {}

  @Override
  public boolean hasTop() {
    return !done;
  }

  @Override
  public void next() throws IOException {
    done = true;
  }

  @Override
  public void seek(
      final Range range,
      final Collection<ByteSequence> columnFamilies,
      final boolean inclusive) throws IOException {}

  @Override
  public Key getTopKey() {
    return new Key();
  }

  @Override
  public Value getTopValue() {
    return new Value(
        StringUtils.stringToBinary(
            VersionUtils.asLineDelimitedString(VersionUtils.getVersionInfo())));
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(final IteratorEnvironment env) {
    return null;
  }
}
