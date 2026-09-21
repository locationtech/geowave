/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.driver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Runs a {@link Reducer} over a list of already-grouped inputs and collects what it writes.
 *
 * <p> The replacement for {@code org.apache.hadoop.mrunit.mapreduce.ReduceDriver}; see
 * {@link MapDriver} for why MRUnit had to go. Groups are reduced in the order they were added, and
 * no sorting or grouping comparator is applied, which matches how the tests here use it: they build
 * the groups themselves from a mapper run.
 */
public class ReduceDriver<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  private final Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer;
  private final Configuration configuration = new Configuration();
  private final List<Pair<KEYIN, List<VALUEIN>>> inputs = new ArrayList<>();

  private ReduceDriver(final Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer) {
    this.reducer = reducer;
  }

  public static <KEYIN, VALUEIN, KEYOUT, VALUEOUT> ReduceDriver<KEYIN, VALUEIN, KEYOUT, VALUEOUT> newReduceDriver(
      final Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer) {
    return new ReduceDriver<>(reducer);
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public ReduceDriver<KEYIN, VALUEIN, KEYOUT, VALUEOUT> addInput(
      final KEYIN key,
      final List<VALUEIN> values) {
    inputs.add(new Pair<>(key, values));
    return this;
  }

  public ReduceDriver<KEYIN, VALUEIN, KEYOUT, VALUEOUT> addAll(
      final List<Pair<KEYIN, List<VALUEIN>>> groups) {
    inputs.addAll(groups);
    return this;
  }

  @SuppressWarnings("unchecked")
  public List<Pair<KEYOUT, VALUEOUT>> run() throws IOException {
    final List<Pair<KEYOUT, VALUEOUT>> outputs = new ArrayList<>();
    final Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context =
        mock(Reducer.Context.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));

    final Iterator<Pair<KEYIN, List<VALUEIN>>> remaining = inputs.iterator();
    final AtomicReference<Pair<KEYIN, List<VALUEIN>>> current = new AtomicReference<>();

    try {
      when(context.getConfiguration()).thenReturn(configuration);
      when(context.nextKey()).thenAnswer(invocation -> {
        current.set(remaining.hasNext() ? remaining.next() : null);
        return current.get() != null;
      });
      when(context.getCurrentKey()).thenAnswer(invocation -> current.get().getFirst());
      when(context.getValues()).thenAnswer(invocation -> current.get().getSecond());
      doAnswer(invocation -> {
        outputs.add(
            new Pair<>(
                HadoopSerializationCopier.copy((KEYOUT) invocation.getArgument(0), configuration),
                HadoopSerializationCopier.copy(
                    (VALUEOUT) invocation.getArgument(1),
                    configuration)));
        return null;
      }).when(context).write(any(), any());

      reducer.run(context);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    return outputs;
  }
}
