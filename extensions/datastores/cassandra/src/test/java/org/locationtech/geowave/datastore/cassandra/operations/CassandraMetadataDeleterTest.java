/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.operations;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;

public class CassandraMetadataDeleterTest {
  private static final byte[] GROUP = bytes("Atype");

  private static byte[] bytes(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  // The reader returns the given entries for any query.
  @SuppressWarnings("unchecked")
  private static CassandraOperations operations(final GeoWaveMetadata... found) {
    final CassandraOperations operations = mock(CassandraOperations.class, RETURNS_DEEP_STUBS);
    when(operations.createMetadataReader(any())).thenReturn(
        query -> new CloseableIterator.Wrapper<>(Arrays.asList(found).iterator()));
    final Delete delete = mock(Delete.class, RETURNS_DEEP_STUBS);
    final ColumnRelationBuilder<Delete> relation = mock(ColumnRelationBuilder.class);
    when(relation.isEqualTo(any())).thenReturn(delete);
    when(delete.whereColumn(anyString())).thenReturn(relation);
    final DeleteSelection selection = mock(DeleteSelection.class);
    when(selection.whereColumn(anyString())).thenReturn(relation);
    when(operations.getDelete(any())).thenReturn(selection);
    return operations;
  }

  private static boolean delete(final CassandraOperations operations, final MetadataQuery query) {
    final boolean deleted =
        new CassandraMetadataDeleter(operations, MetadataType.STATISTIC_VALUES).delete(query);
    verify(operations, never()).dropMetadataTable(any());
    return deleted;
  }

  private static MetadataQuery range() {
    return new MetadataQuery(
        new ByteArrayRange[] {new ByteArrayRange(bytes("COUNT|alpha|x1"), bytes("COUNT|alpha|x9"))},
        GROUP);
  }

  private static MetadataQuery prefix() {
    return new MetadataQuery(bytes("COUNT|alpha|x"), GROUP, true);
  }

  @Test
  public void rangeAndPrefixDeletesReportWhetherAnythingMatched() {
    final GeoWaveMetadata entry =
        new GeoWaveMetadata(bytes("COUNT|alpha|x5"), GROUP, null, bytes("value"));
    assertFalse(delete(operations(), range()));
    assertTrue(delete(operations(entry), range()));
    assertFalse(delete(operations(), prefix()));
    assertTrue(delete(operations(entry), prefix()));
  }
}
