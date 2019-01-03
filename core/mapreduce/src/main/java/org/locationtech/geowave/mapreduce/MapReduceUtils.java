/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.util.List;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

public class MapReduceUtils {
  public static List<String> idsFromAdapters(final List<DataTypeAdapter<Object>> adapters) {
    return Lists.transform(adapters, new Function<DataTypeAdapter<Object>, String>() {
      @Override
      public String apply(final DataTypeAdapter<Object> adapter) {
        return adapter == null ? "" : adapter.getTypeName();
      }
    });
  }
}
