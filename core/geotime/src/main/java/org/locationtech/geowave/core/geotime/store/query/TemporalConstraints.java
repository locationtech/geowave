/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.locationtech.geowave.core.index.VarintUtils;

public class TemporalConstraints {
  private LinkedList<TemporalRange> constraints = new LinkedList<>();
  private String name;

  public static final TemporalRange FULL_RANGE =
      new TemporalRange(TemporalRange.START_TIME, TemporalRange.END_TIME);

  public TemporalConstraints() {}

  public String getName() {
    return name;
  }

  public void empty() {
    constraints.clear();
  }

  public TemporalConstraints(final String name) {
    this.name = name;
  }

  public TemporalConstraints(final List<TemporalRange> ranges, final String name) {
    constraints.addAll(ranges);
    this.name = name;
  }

  public TemporalConstraints(final TemporalRange range, final String name) {
    constraints.add(range);
    this.name = name;
  }

  public void replaceWithIntersections(final TemporalConstraints constraints) {
    this.constraints = TemporalConstraints.findIntersections(this, constraints).constraints;
  }

  public void replaceWithMerged(final TemporalConstraints constraints) {
    this.constraints = TemporalConstraints.merge(this, constraints).constraints;
  }

  public void add(final TemporalRange range) {
    int pos = 0;
    TemporalRange nextNeighbor = null;
    for (final TemporalRange aRange : constraints) {
      nextNeighbor = aRange;
      if (nextNeighbor.getStartTime().after(range.getStartTime())) {
        break;
      } else if (nextNeighbor.getEndTime().after(range.getStartTime())
          || nextNeighbor.getEndTime().equals(range.getStartTime())) {
        if (range.getEndTime().before(nextNeighbor.getEndTime())) {
          // subsummed
          return;
        } else {
          // replaced with larger range
          constraints.set(pos, new TemporalRange(nextNeighbor.getStartTime(), range.getEndTime()));
          return;
        }
      }
      pos++;
    }
    if ((nextNeighbor != null) && nextNeighbor.getStartTime().before(range.getEndTime())) {
      constraints.add(
          pos,
          new TemporalRange(
              range.getStartTime(),
              TemporalConstraints.max(nextNeighbor.getEndTime(), range.getEndTime())));
    } else {
      constraints.add(pos, range);
    }
  }

  public static final Date max(final Date one, final Date two) {
    return one.before(two) ? two : one;
  }

  public static final Date min(final Date one, final Date two) {
    return one.before(two) ? one : two;
  }

  public Date getMinOr(final Date min, final int exclusivityIncrement) {
    return (constraints.isEmpty()) ? min
        : exclusivityIncrement == 0 ? constraints.getFirst().getStartTime()
            : new Date(constraints.getFirst().getStartTime().getTime() + exclusivityIncrement);
  }

  public Date getMaxOr(final Date max, final int exclusivityIncrement) {
    return (constraints.isEmpty()) ? max
        : exclusivityIncrement == 0 ? constraints.getLast().getEndTime()
            : new Date(constraints.getLast().getEndTime().getTime() + exclusivityIncrement);
  }

  public boolean isEmpty() {
    return constraints.isEmpty();
  }

  public TemporalRange getEndRange() {
    return (constraints.isEmpty()) ? FULL_RANGE : constraints.getLast();
  }

  public TemporalRange getStartRange() {
    return (constraints.isEmpty()) ? FULL_RANGE : constraints.getFirst();
  }

  public List<TemporalRange> getRanges() {
    return constraints == null ? Collections.<TemporalRange>emptyList() : constraints;
  }

  public static final TemporalConstraints findIntersections(
      final TemporalConstraints sideL,
      final TemporalConstraints sideR) {

    if (sideL.constraints.isEmpty()) {
      return sideR;
    }
    if (sideR.constraints.isEmpty()) {
      return sideL;
    }

    final TemporalConstraints newSet = new TemporalConstraints(sideL.name);

    for (final TemporalRange lRange : sideL.constraints) {
      for (final TemporalRange rRange : sideR.constraints) {
        if (lRange.getEndTime().before(rRange.getStartTime())
            || rRange.getEndTime().before(lRange.getStartTime())) {
          continue;
        }
        newSet.add(
            new TemporalRange(
                max(lRange.getStartTime(), rRange.getStartTime()),
                min(lRange.getEndTime(), rRange.getEndTime())));
      }
    }
    return newSet;
  }

  public static final TemporalConstraints merge(
      final TemporalConstraints left,
      final TemporalConstraints right) {
    if (left.isEmpty()) {
      return right;
    }
    if (right.isEmpty()) {
      return left;
    }

    final TemporalConstraints newSetOfRanges = new TemporalConstraints(left.name);
    newSetOfRanges.constraints.addAll(left.constraints);
    for (final TemporalRange range : right.constraints) {
      newSetOfRanges.add(range);
    }
    return newSetOfRanges;
  }

  public byte[] toBinary() {
    int bufferSize = VarintUtils.unsignedIntByteLength(constraints.size());
    for (final TemporalRange range : constraints) {
      bufferSize += range.getBufferSize();
    }
    final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    VarintUtils.writeUnsignedInt(constraints.size(), buffer);

    for (final TemporalRange range : constraints) {
      range.toBinary(buffer);
    }

    return buffer.array();
  }

  public void fromBinary(final byte[] data) {
    final ByteBuffer buffer = ByteBuffer.wrap(data);

    final int s = VarintUtils.readUnsignedInt(buffer);
    for (int i = 0; i < s; i++) {
      final TemporalRange range = new TemporalRange();
      range.fromBinary(buffer);
      add(range);
    }
  }

  @Override
  public String toString() {
    return "TemporalConstraints [constraints=" + constraints + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((constraints == null) ? 0 : constraints.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final TemporalConstraints other = (TemporalConstraints) obj;
    if (constraints == null) {
      if (other.constraints != null) {
        return false;
      }
    } else if (!constraints.equals(other.constraints)) {
      return false;
    }
    return true;
  }
}
