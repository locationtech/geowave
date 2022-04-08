/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

public interface PartitionIndexStrategy<QueryRangeType extends IndexConstraints, EntryRangeType>
    extends
    IndexStrategy<QueryRangeType, EntryRangeType> {
  byte[][] getInsertionPartitionKeys(EntryRangeType insertionData);

  byte[][] getQueryPartitionKeys(QueryRangeType queryData, IndexMetaData... hints);

  /**
   * * Get the offset in bytes before the dimensional index. This can accounts for tier IDs and bin
   * IDs
   *
   * @return the byte offset prior to the dimensional index
   */
  int getPartitionKeyLength();

  default byte[][] getPredefinedSplits() {
    return new byte[0][];
  }
}
