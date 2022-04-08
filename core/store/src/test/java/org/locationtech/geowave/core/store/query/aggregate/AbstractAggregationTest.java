/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import java.util.List;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

public abstract class AbstractAggregationTest {

  /**
   * Aggregate given objects into a given aggregation.
   *
   * Internally, this splits the objectsToAggregate and gives it to separate aggregations, and
   * returns the merged results.
   *
   * @param aggregation The aggregation to give data to for testing.
   * @param objectsToAggregate The test data to feed into the aggregation
   * @return The results of aggregating the data.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected <P extends Persistable, R, T> R aggregateObjects(
      final DataTypeAdapter<T> adapter,
      final Aggregation<P, R, T> aggregation,
      final List<T> objectsToAggregate) {
    final byte[] aggregationBytes = PersistenceUtils.toBinary(aggregation);
    final byte[] aggregationParameters = PersistenceUtils.toBinary(aggregation.getParameters());
    final Aggregation<P, R, T> agg1 = (Aggregation) PersistenceUtils.fromBinary(aggregationBytes);
    final Aggregation<P, R, T> agg2 = (Aggregation) PersistenceUtils.fromBinary(aggregationBytes);
    agg1.setParameters((P) PersistenceUtils.fromBinary(aggregationParameters));
    agg2.setParameters((P) PersistenceUtils.fromBinary(aggregationParameters));
    for (int i = 0; i < objectsToAggregate.size(); i++) {
      if ((i % 2) == 0) {
        agg1.aggregate(adapter, objectsToAggregate.get(i));
      } else {
        agg2.aggregate(adapter, objectsToAggregate.get(i));
      }
    }
    final byte[] agg1ResultBinary = agg1.resultToBinary(agg1.getResult());
    final byte[] agg2ResultBinary = agg2.resultToBinary(agg2.getResult());
    final R agg1Result = agg1.resultFromBinary(agg1ResultBinary);
    final R agg2Result = agg2.resultFromBinary(agg2ResultBinary);

    return aggregation.merge(agg1Result, agg2Result);
  }
}
