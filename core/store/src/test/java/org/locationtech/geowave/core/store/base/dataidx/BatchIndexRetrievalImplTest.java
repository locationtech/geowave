/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base.dataidx;

import static org.junit.Assert.assertEquals;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Test;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.RowReader;

public class BatchIndexRetrievalImplTest {
  private static final short ADAPTER_ID = 7;
  private static final List<String> DATA_IDS = Arrays.asList("a", "b", "c", "d", "e");
  private static final String NO_ROW = "no row";

  @Test
  public void testMissingDataIdDoesNotShiftLaterResults() throws Exception {
    // every datastore's batch read leaves out an ID it has no row for; leaving out the first
    // requested ID is the case that shifts every later row onto the wrong request
    assertOutcomes(
        requested -> new FakeReader(requested.subList(1, requested.size())),
        (position, id) -> position == 0 ? NO_ROW : id);
  }

  @Test
  public void testResultsNeedNotComeBackInRequestOrder() throws Exception {
    assertOutcomes(requested -> {
      final List<String> reversed = new ArrayList<>(requested);
      Collections.reverse(reversed);
      return new FakeReader(reversed);
    }, (position, id) -> id);
  }

  @Test
  public void testRepeatedRowIsNotGivenToTheNextRequest() throws Exception {
    assertOutcomes(requested -> {
      final List<String> firstTwice = new ArrayList<>(requested);
      firstTwice.add(0, requested.get(0));
      return new FakeReader(firstTwice);
    }, (position, id) -> id);
  }

  @Test
  public void testReadFailureCompletesTheRestOfTheBatchExceptionally() throws Exception {
    assertOutcomes(
        requested -> new FakeReader(requested.subList(0, 1)).failingAfterLastRow(
            new IllegalStateException("read failed")),
        (position, id) -> position == 0 ? id : "failed: read failed");
  }

  @Test
  public void testFailureToCloseTheReaderStillCompletesEveryRequest() throws Exception {
    assertOutcomes(
        requested -> new FakeReader(requested.subList(1, requested.size())).failingOnClose(
            new IllegalStateException("close failed")),
        (position, id) -> position == 0 ? NO_ROW : id);
  }

  /**
   * Requests every ID in one batch from a data index that serves it with the reader chosen by
   * readerForRequestedIds, and compares what each request was given with expectedForPositionAndId,
   * in the order the batch requested them.
   */
  private static void assertOutcomes(
      final Function<List<String>, RowReader<GeoWaveRow>> readerForRequestedIds,
      final BiFunction<Integer, String, String> expectedForPositionAndId) throws Exception {
    final CompletableFuture<List<String>> requestedIds = new CompletableFuture<>();
    final BatchIndexRetrievalImpl retrieval = new BatchIndexRetrievalImpl(dataIndex(requested -> {
      requestedIds.complete(requested);
      return readerForRequestedIds.apply(requested);
    }), null, null, null, null, null, new String[0], DATA_IDS.size());
    final Map<String, CompletableFuture<GeoWaveValue[]>> futures = new LinkedHashMap<>();
    // the last request fills the batch, which flushes it
    for (final String id : DATA_IDS) {
      futures.put(id, retrieval.getDataAsync(ADAPTER_ID, StringUtils.stringToBinary(id)));
    }
    final List<String> requested = requestedIds.get(30, TimeUnit.SECONDS);
    assertEquals(new HashSet<>(DATA_IDS), new HashSet<>(requested));
    try {
      CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).get(
          10,
          TimeUnit.SECONDS);
    } catch (final ExecutionException | TimeoutException e) {
      // described request by request below
    }
    final List<String> expected = new ArrayList<>();
    final List<String> actual = new ArrayList<>();
    for (int i = 0; i < requested.size(); i++) {
      final String id = requested.get(i);
      expected.add(id + ": " + expectedForPositionAndId.apply(i, id));
      actual.add(id + ": " + outcome(futures.get(id)));
    }
    assertEquals(String.join(", ", expected), String.join(", ", actual));
  }

  private static String outcome(final CompletableFuture<GeoWaveValue[]> future) {
    if (!future.isDone()) {
      return "incomplete";
    }
    try {
      final GeoWaveValue[] values = future.join();
      return values == null ? NO_ROW : StringUtils.stringFromBinary(values[0].getValue());
    } catch (final CompletionException e) {
      return "failed: " + e.getCause().getMessage();
    }
  }

  private static DataStoreOperations dataIndex(
      final Function<List<String>, RowReader<GeoWaveRow>> readerForRequestedIds) {
    return (DataStoreOperations) Proxy.newProxyInstance(
        DataStoreOperations.class.getClassLoader(),
        new Class<?>[] {DataStoreOperations.class},
        (proxy, method, args) -> {
          if (method.getName().equals("createReader")
              && (args[0] instanceof DataIndexReaderParams)) {
            return readerForRequestedIds.apply(
                Arrays.stream(((DataIndexReaderParams) args[0]).getDataIds()).map(
                    StringUtils::stringFromBinary).collect(Collectors.toList()));
          }
          throw new UnsupportedOperationException(method.toString());
        });
  }

  /** Returns a row for each of the given IDs, whose value is that ID */
  private static class FakeReader implements RowReader<GeoWaveRow> {
    private final Iterator<String> ids;
    private RuntimeException failureAfterLastRow;
    private RuntimeException failureOnClose;

    private FakeReader(final List<String> ids) {
      this.ids = new ArrayList<>(ids).iterator();
    }

    private FakeReader failingAfterLastRow(final RuntimeException failure) {
      failureAfterLastRow = failure;
      return this;
    }

    private FakeReader failingOnClose(final RuntimeException failure) {
      failureOnClose = failure;
      return this;
    }

    @Override
    public boolean hasNext() {
      if (ids.hasNext()) {
        return true;
      }
      if (failureAfterLastRow != null) {
        throw failureAfterLastRow;
      }
      return false;
    }

    @Override
    public GeoWaveRow next() {
      final byte[] id = StringUtils.stringToBinary(ids.next());
      return new GeoWaveRowImpl(
          new GeoWaveKeyImpl(id, ADAPTER_ID, new byte[0], new byte[0], 0),
          new GeoWaveValue[] {new GeoWaveValueImpl(new byte[0], new byte[0], id)});
    }

    @Override
    public void close() {
      if (failureOnClose != null) {
        throw failureOnClose;
      }
    }
  }
}
