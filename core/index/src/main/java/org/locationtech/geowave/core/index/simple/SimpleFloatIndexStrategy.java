package org.locationtech.geowave.core.index.simple;

import org.locationtech.geowave.core.index.lexicoder.Lexicoders;

public class SimpleFloatIndexStrategy extends SimpleNumericIndexStrategy<Float> {

  public SimpleFloatIndexStrategy() {
    super(Lexicoders.FLOAT);
  }

  @Override
  protected Float cast(final double value) {
    return (float) value;
  }

  @Override
  protected boolean isInteger() {
    return false;
  }
}
