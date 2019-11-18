/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
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

public abstract class AbstractAggregationTest<P extends Persistable, R, T> {

  @SuppressWarnings({"unchecked", "rawtypes"})
  public R aggregateObjects(Aggregation<P, R, T> aggregation, List<T> objectsToAggregate) {
    byte[] aggregationBytes = PersistenceUtils.toBinary(aggregation);
    byte[] aggregationParameters = PersistenceUtils.toBinary(aggregation.getParameters());
    Aggregation<P, R, T> agg1 = (Aggregation) PersistenceUtils.fromBinary(aggregationBytes);
    Aggregation<P, R, T> agg2 = (Aggregation) PersistenceUtils.fromBinary(aggregationBytes);
    agg1.setParameters((P) PersistenceUtils.fromBinary(aggregationParameters));
    agg2.setParameters((P) PersistenceUtils.fromBinary(aggregationParameters));
    for (int i = 0; i < objectsToAggregate.size(); i++) {
      if (i % 2 == 0) {
        agg1.aggregate(objectsToAggregate.get(i));
      } else {
        agg2.aggregate(objectsToAggregate.get(i));
      }
    }
    byte[] agg1ResultBinary = agg1.resultToBinary(agg1.getResult());
    byte[] agg2ResultBinary = agg2.resultToBinary(agg2.getResult());
    R agg1Result = agg1.resultFromBinary(agg1ResultBinary);
    R agg2Result = agg2.resultFromBinary(agg2ResultBinary);

    return aggregation.merge(agg1Result, agg2Result);
  }

}
