/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.lang.ArrayUtils;
import org.locationtech.geowave.core.index.ByteArray;
import com.google.common.primitives.Bytes;

/**
 * Static utility methods useful for binning strategies
 *
 */
public class BinningStrategyUtils {
  /**
   * in the case of queries of composite or multi-field bins, we need to query all combinations of
   * individual bins
   *
   * @param individualBins the individual bin
   * @param concatenateByteArrays a method to concatenate the bins, useful for different nuances of
   *        encoding
   * @return all combinations of the concatenated individual bins
   */
  public static ByteArray[] getAllCombinations(
      final ByteArray[][] individualBins,
      final Function<ByteArray[], ByteArray> concatenateByteArrays) {
    final List<ByteArray[]> combinedConstraintCombos = new ArrayList<>();
    combos(0, individualBins, new ByteArray[0], combinedConstraintCombos);
    return combinedConstraintCombos.stream().map(concatenateByteArrays).toArray(ByteArray[]::new);
  }

  private static void combos(
      final int pos,
      final ByteArray[][] c,
      final ByteArray[] soFar,
      final List<ByteArray[]> finalList) {
    if (pos == c.length) {
      finalList.add(soFar);
      return;
    }
    for (int i = 0; i != c[pos].length; i++) {
      combos(pos + 1, c, (ByteArray[]) ArrayUtils.add(soFar, c[pos][i]), finalList);
    }
  }
}
