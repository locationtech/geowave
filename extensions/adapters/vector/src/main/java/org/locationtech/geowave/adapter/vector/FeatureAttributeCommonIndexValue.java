package org.locationtech.geowave.adapter.vector;

import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

public class FeatureAttributeCommonIndexValue implements CommonIndexValue {
  private final Number value;
  private byte[] visibility;

  public FeatureAttributeCommonIndexValue(final Number value, final byte[] visibility) {
    this.value = value;
    this.visibility = visibility;
  }

  public Number getValue() {
    return value;
  }

  @Override
  public byte[] getVisibility() {
    return visibility;
  }

  @Override
  public void setVisibility(final byte[] visibility) {
    this.visibility = visibility;
  }

  @Override
  public boolean overlaps(final NumericDimensionField[] field, final NumericData[] rangeData) {
    assert (field[0] instanceof FeatureAttributeCommonIndexValue);
    return (value != null)
        && (value.doubleValue() <= rangeData[0].getMax())
        && (value.doubleValue() >= rangeData[0].getMin());
  }
}
