package org.locationtech.geowave.core.store.dimension;

import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.bin.BinRange;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

abstract public class AbstractNumericDimensionField<T extends CommonIndexValue> implements
    NumericDimensionField<T> {
  protected NumericDimensionDefinition baseDefinition;

  public AbstractNumericDimensionField() {}

  public AbstractNumericDimensionField(final NumericDimensionDefinition baseDefinition) {
    this.baseDefinition = baseDefinition;
  }

  protected void setBaseDefinition(final NumericDimensionDefinition baseDefinition) {
    this.baseDefinition = baseDefinition;
  }

  @Override
  public double getRange() {
    return baseDefinition.getRange();
  }

  @Override
  public double normalize(final double value) {
    return baseDefinition.normalize(value);
  }

  @Override
  public double denormalize(final double value) {
    return baseDefinition.denormalize(value);
  }

  @Override
  public BinRange[] getNormalizedRanges(final NumericData range) {
    return baseDefinition.getNormalizedRanges(range);
  }

  @Override
  public NumericRange getDenormalizedRange(final BinRange range) {
    return baseDefinition.getDenormalizedRange(range);
  }

  @Override
  public int getFixedBinIdSize() {
    return baseDefinition.getFixedBinIdSize();
  }

  @Override
  public NumericRange getBounds() {
    return baseDefinition.getBounds();
  }

  @Override
  public NumericData getFullRange() {
    return baseDefinition.getFullRange();
  }

  @Override
  public NumericDimensionDefinition getBaseDefinition() {
    return baseDefinition;
  }
}
