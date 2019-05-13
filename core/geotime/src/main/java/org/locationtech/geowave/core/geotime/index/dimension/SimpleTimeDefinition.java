package org.locationtech.geowave.core.geotime.index.dimension;

import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;

public class SimpleTimeDefinition extends BasicDimensionDefinition {

  public SimpleTimeDefinition() {
    super(Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Override
  public byte[] toBinary() {
    return new byte[0];
  }

  @Override
  public void fromBinary(final byte[] bytes) {}

}
