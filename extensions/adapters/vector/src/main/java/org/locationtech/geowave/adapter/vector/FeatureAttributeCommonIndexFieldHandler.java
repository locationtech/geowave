package org.locationtech.geowave.adapter.vector;

import org.locationtech.geowave.core.store.adapter.IndexFieldHandler;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

public class FeatureAttributeCommonIndexFieldHandler implements
    IndexFieldHandler<SimpleFeature, FeatureAttributeCommonIndexValue, Object> {
  protected final AttributeDescriptor attrDesc;
  private final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler;

  public FeatureAttributeCommonIndexFieldHandler(final AttributeDescriptor attrDesc) {
    this.attrDesc = attrDesc;
    visibilityHandler = null;
  }

  public FeatureAttributeCommonIndexFieldHandler(
      final AttributeDescriptor attrDesc,
      final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler) {
    super();
    this.attrDesc = attrDesc;
    this.visibilityHandler = visibilityHandler;
  }

  @Override
  public String[] getNativeFieldNames() {
    return new String[] {attrDesc.getLocalName()};
  }

  @Override
  public FeatureAttributeCommonIndexValue toIndexValue(final SimpleFeature row) {
    final Object value = row.getAttribute(attrDesc.getName());
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      byte[] visibility;
      if (visibilityHandler != null) {
        visibility = visibilityHandler.getVisibility(row, attrDesc.getLocalName(), value);
      } else {
        visibility = new byte[] {};
      }
      return new FeatureAttributeCommonIndexValue((Number) value, visibility);
    }
    return null;
  }

  @Override
  public FeatureAttributeCommonIndexValue toIndexValue(
      final PersistentDataset<Object> adapterPersistenceEncoding) {
    final Number number = (Number) adapterPersistenceEncoding.getValue(attrDesc.getLocalName());
    // visibility is unnecessary because this only happens after the number is read (its only used
    // in reconstructing common index values when using a secondary index)
    return new FeatureAttributeCommonIndexValue(number, null);
  }

  @Override
  public PersistentValue<Object>[] toNativeValues(
      final FeatureAttributeCommonIndexValue indexValue) {
    return new PersistentValue[] {
        new PersistentValue<>(attrDesc.getLocalName(), indexValue.getValue())};
  }

}
