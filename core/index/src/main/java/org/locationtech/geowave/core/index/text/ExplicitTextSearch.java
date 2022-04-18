/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.text;

import java.util.EnumSet;
import java.util.List;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import com.google.common.collect.Lists;

/**
 * Explicitly queries a set of text ranges.
 */
public class ExplicitTextSearch implements TextConstraints {

  private List<MultiDimensionalTextData> indexData;

  public ExplicitTextSearch() {}

  public ExplicitTextSearch(final List<MultiDimensionalTextData> indexData) {
    this.indexData = indexData;
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(indexData);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void fromBinary(final byte[] bytes) {
    indexData = (List) PersistenceUtils.fromBinaryAsList(bytes);
  }

  @Override
  public QueryRanges getQueryRanges(
      final EnumSet<TextSearchType> supportedSearchTypes,
      final int nCharacterGrams) {
    final List<QueryRanges> ranges = Lists.newArrayListWithCapacity(indexData.size());
    for (final MultiDimensionalTextData data : indexData) {
      ranges.add(TextIndexUtils.getQueryRanges(data));
    }
    if (ranges.size() == 1) {
      return ranges.get(0);
    }
    return new QueryRanges(ranges);
  }

}
