/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.types;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** */
public class HelperClass {

  public static <T> Set<String> buildSet(final Map<String, ValidateObject<T>> expectedResults) {
    final HashSet<String> set = new HashSet<>();
    for (final String key : expectedResults.keySet()) {
      set.add(key);
    }
    return set;
  }
}
