/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics.binning;

import java.util.concurrent.TimeUnit;

/**
 * Fixed-width UTC time buckets. HOUR and DAY are aligned to the Unix epoch; WEEK is anchored at
 * Monday 00:00 UTC.
 */
public enum EpochInterval {
  HOUR(TimeUnit.HOURS.toMillis(1), 0L),
  DAY(TimeUnit.DAYS.toMillis(1), 0L),
  // 1970-01-05T00:00:00Z, the first Monday after the epoch
  WEEK(TimeUnit.DAYS.toMillis(7), TimeUnit.DAYS.toMillis(4));

  private final long widthMillis;
  private final long anchorMillis;

  EpochInterval(final long widthMillis, final long anchorMillis) {
    this.widthMillis = widthMillis;
    this.anchorMillis = anchorMillis;
  }

  public long getWidthMillis() {
    return widthMillis;
  }

  public long bucketStart(final long millis) {
    return (Math.floorDiv(millis - anchorMillis, widthMillis) * widthMillis) + anchorMillis;
  }
}
