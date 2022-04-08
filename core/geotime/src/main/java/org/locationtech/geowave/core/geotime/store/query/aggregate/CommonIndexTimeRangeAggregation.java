/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.query.aggregate.CommonIndexAggregation;
import org.threeten.extra.Interval;

public class CommonIndexTimeRangeAggregation<P extends Persistable> extends
    TimeRangeAggregation<P, CommonIndexedPersistenceEncoding> implements
    CommonIndexAggregation<P, Interval> {

  @Override
  protected Interval getInterval(final CommonIndexedPersistenceEncoding entry) {
    return null;
  }
}
