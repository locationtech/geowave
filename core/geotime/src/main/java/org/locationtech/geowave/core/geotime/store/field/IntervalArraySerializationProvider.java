/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.field;

import org.locationtech.geowave.core.geotime.store.field.IntervalSerializationProvider.IntervalReader;
import org.locationtech.geowave.core.geotime.store.field.IntervalSerializationProvider.IntervalWriter;
import org.locationtech.geowave.core.store.data.field.ArrayReader;
import org.locationtech.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.threeten.extra.Interval;

public class IntervalArraySerializationProvider implements
    FieldSerializationProviderSpi<Interval[]> {
  @Override
  public FieldReader<Interval[]> getFieldReader() {
    return new IntervalArrayReader();
  }

  @Override
  public FieldWriter<Interval[]> getFieldWriter() {
    return new IntervalArrayWriter();
  }

  private static class IntervalArrayReader extends ArrayReader<Interval> {
    public IntervalArrayReader() {
      super(new IntervalReader());
    }
  }

  private static class IntervalArrayWriter extends VariableSizeObjectArrayWriter<Interval> {
    public IntervalArrayWriter() {
      super(new IntervalWriter());
    }
  }
}
