/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.splits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RangeLocationPair {
  private GeoWaveRowRange range;
  private String location;
  private double cardinality;

  protected RangeLocationPair() {}

  public RangeLocationPair(final GeoWaveRowRange range, final double cardinality) {
    this(range, "", cardinality);
  }

  public RangeLocationPair(
      final GeoWaveRowRange range,
      final String location,
      final double cardinality) {
    this.location = location;
    this.range = range;
    this.cardinality = cardinality;
  }

  public double getCardinality() {
    return cardinality;
  }

  public GeoWaveRowRange getRange() {
    return range;
  }

  public String getLocation() {
    return location;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((location == null) ? 0 : location.hashCode());
    result = (prime * result) + ((range == null) ? 0 : range.hashCode());
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
    final RangeLocationPair other = (RangeLocationPair) obj;
    if (location == null) {
      if (other.location != null) {
        return false;
      }
    } else if (!location.equals(other.location)) {
      return false;
    }
    if (range == null) {
      if (other.range != null) {
        return false;
      }
    } else if (!range.equals(other.range)) {
      return false;
    }
    return true;
  }

  public void readFields(final DataInput in)
      throws IOException, InstantiationException, IllegalAccessException {
    final boolean nullRange = in.readBoolean();
    if (nullRange) {
      range = null;
    } else {
      range = new GeoWaveRowRange();
      range.readFields(in);
    }
    location = in.readUTF();
    cardinality = in.readDouble();
  }

  public void write(final DataOutput out) throws IOException {
    out.writeBoolean(range == null);
    if (range != null) {
      range.write(out);
    }
    out.writeUTF(location);
    out.writeDouble(cardinality);
  }
}
