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
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Runs a {@link Mapper} over a list of inputs and collects what it writes.
 *
 * <p> This replaces {@code org.apache.hadoop.mrunit.mapreduce.MapDriver}. Apache MRUnit was retired
 * to the Attic in 2016 and pins mockito 1.x, whose cglib cannot create a mock on any JDK above 8.
 * Only the handful of {@code Context} methods that {@link Mapper#run} and the mappers under test
 * actually call are stubbed; everything else returns a smart null, so an unanticipated call fails
 * where it happens rather than quietly reading as null.
 */
public class MapDriver<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  private final Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper;
  private final Configuration configuration = new Configuration();
  private final List<Pair<KEYIN, VALUEIN>> inputs = new ArrayList<>();

  private MapDriver(final Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper) {
    this.mapper = mapper;
  }

  public static <KEYIN, VALUEIN, KEYOUT, VALUEOUT> MapDriver<KEYIN, VALUEIN, KEYOUT, VALUEOUT> newMapDriver(
      final Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper) {
    return new MapDriver<>(mapper);
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public MapDriver<KEYIN, VALUEIN, KEYOUT, VALUEOUT> addInput(
      final KEYIN key,
      final VALUEIN value) {
    inputs.add(new Pair<>(key, value));
    return this;
  }

  public MapDriver<KEYIN, VALUEIN, KEYOUT, VALUEOUT> withInput(
      final KEYIN key,
      final VALUEIN value) {
    return addInput(key, value);
  }

  @SuppressWarnings("unchecked")
  public List<Pair<KEYOUT, VALUEOUT>> run() throws IOException {
    final List<Pair<KEYOUT, VALUEOUT>> outputs = new ArrayList<>();
    final Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context =
        mock(Mapper.Context.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));

    final Iterator<Pair<KEYIN, VALUEIN>> remaining = inputs.iterator();
    final AtomicReference<Pair<KEYIN, VALUEIN>> current = new AtomicReference<>();

    try {
      when(context.getConfiguration()).thenReturn(configuration);
      when(context.nextKeyValue()).thenAnswer(invocation -> {
        current.set(remaining.hasNext() ? remaining.next() : null);
        return current.get() != null;
      });
      when(context.getCurrentKey()).thenAnswer(invocation -> current.get().getFirst());
      when(context.getCurrentValue()).thenAnswer(invocation -> current.get().getSecond());
      doAnswer(invocation -> {
        outputs.add(
            new Pair<>(
                HadoopSerializationCopier.copy((KEYOUT) invocation.getArgument(0), configuration),
                HadoopSerializationCopier.copy(
                    (VALUEOUT) invocation.getArgument(1),
                    configuration)));
        return null;
      }).when(context).write(any(), any());

      mapper.run(context);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    return outputs;
  }
}
