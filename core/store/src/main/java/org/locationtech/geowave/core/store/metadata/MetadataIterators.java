/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.metadata;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.data.visibility.VisibilityExpression;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import com.google.common.collect.Iterators;

public class MetadataIterators {

  public static CloseableIterator<GeoWaveMetadata> clientVisibilityFilter(
      final CloseableIterator<GeoWaveMetadata> source,
      final String... authorizations) {
    if (authorizations != null) {
      final Set<String> authorizationSet = new HashSet<>(Arrays.asList(authorizations));
      return new CloseableIteratorWrapper<>(
          source,
          Iterators.filter(source, input -> isVisible(input, authorizationSet)));
    }
    return source;
  }

  public static CloseableIterator<GeoWaveMetadata> clientPrefixFilter(
      final CloseableIterator<GeoWaveMetadata> source,
      final MetadataQuery query) {
    if (query.hasPrimaryId()) {
      return new CloseableIteratorWrapper<>(
          source,
          Iterators.filter(source, input -> startsWith(input, query)));
    }
    return source;
  }

  public static CloseableIterator<GeoWaveMetadata> clientPrefixAndVisibilityFilter(
      final CloseableIterator<GeoWaveMetadata> source,
      final MetadataQuery query) {
    if (query.getAuthorizations() != null) {
      if (query.hasPrimaryId()) {
        final Set<String> authorizationSet =
            new HashSet<>(Arrays.asList(query.getAuthorizations()));
        return new CloseableIteratorWrapper<>(source, Iterators.filter(source, input -> {
          return isVisible(input, authorizationSet) && startsWith(input, query);
        }));
      } else {
        return clientVisibilityFilter(source, query.getAuthorizations());
      }
    } else if (query.hasPrimaryId()) {
      return clientPrefixFilter(source, query);
    }
    return source;
  }

  private static boolean isVisible(
      final GeoWaveMetadata metadata,
      final Set<String> authorizationSet) {
    String visibility = "";
    if (metadata.getVisibility() != null) {
      visibility = StringUtils.stringFromBinary(metadata.getVisibility());
    }
    return VisibilityExpression.evaluate(visibility, authorizationSet);
  }

  private static boolean startsWith(final GeoWaveMetadata metadata, MetadataQuery query) {
    return ByteArrayUtils.startsWith(metadata.getPrimaryId(), query.getPrimaryId());
  }
}
