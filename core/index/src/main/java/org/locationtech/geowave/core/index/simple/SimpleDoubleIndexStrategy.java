package org.locationtech.geowave.core.index.simple;

import org.locationtech.geowave.core.index.lexicoder.Lexicoders;

public class SimpleDoubleIndexStrategy extends SimpleNumericIndexStrategy<Double> {

  public SimpleDoubleIndexStrategy() {
    super(Lexicoders.DOUBLE);
  }

  @Override
  protected Double cast(final double value) {
    return value;
  }

  @Override
  protected boolean isInteger() {
    return false;
  }
}
