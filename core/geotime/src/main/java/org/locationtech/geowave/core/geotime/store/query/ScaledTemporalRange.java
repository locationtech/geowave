/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class ScaledTemporalRange implements Serializable {
  private static final long serialVersionUID = 1L;
  private static long MILLIS_PER_DAY = 86400000;
  private static long DEFAULT_TIME_RANGE = 365L * MILLIS_PER_DAY; // one year

  private Date startTime = null;
  private Date endTime = null;

  // Default to lat bounds
  private double minVal = 0.0;
  private double maxVal = 180.0;

  private long timeRange = DEFAULT_TIME_RANGE;
  private double timeScale;

  private final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

  public ScaledTemporalRange() {
    updateTimeScale();
  }

  public void setTimeRange(final Date startTime, final Date endTime) {
    this.startTime = startTime;
    this.endTime = endTime;

    updateTimeScale();
  }

  public void setTimeRange(final long millis) {
    timeRange = millis;
    startTime = null;
    endTime = null;

    updateTimeScale();
  }

  public void setValueRange(final double minVal, final double maxVal) {
    this.minVal = minVal;
    this.maxVal = maxVal;

    updateTimeScale();
  }

  public void setTimeScale(final double timeScale) {
    this.timeScale = timeScale;
  }

  private void updateTimeScale() {
    timeScale = (maxVal - minVal) / getTimeRangeMillis();
  }

  public double getTimeScale() {
    return timeScale;
  }

  public long getTimeRangeMillis() {
    if ((startTime == null) || (endTime == null)) {
      return timeRange;
    }

    return endTime.getTime() - startTime.getTime();
  }

  public double timeToValue(final Date time) {
    final long deltaTime = time.getTime() - getTimeMin();

    return minVal + (deltaTime * timeScale);
  }

  public Date valueToTime(final double timeVal) {
    final long timeMillis = (long) (timeVal / timeScale) + getTimeMin();
    cal.setTimeInMillis(timeMillis);

    return cal.getTime();
  }

  private long getTimeMin() {
    if (startTime != null) {
      return startTime.getTime();
    }

    return 0L;
  }

  public Date getStartTime() {
    return startTime;
  }

  public void setStartTime(final Date startTime) {
    this.startTime = startTime;
  }

  public Date getEndTime() {
    return endTime;
  }

  public void setEndTime(final Date endTime) {
    this.endTime = endTime;
  }
}
