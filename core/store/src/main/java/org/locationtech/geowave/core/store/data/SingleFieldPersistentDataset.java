package org.locationtech.geowave.core.store.data;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This is a basic mapping of field ID to native field type. "Native" in this sense can be to either
 * the data adapter or the common index, depending on whether it is in the common index or is an
 * extended field.
 *
 * @param <T> The most specific generalization for the type for all of the values in this dataset.
 */
public class SingleFieldPersistentDataset<T> implements PersistentDataSet<T> {
  private String fieldName;
  private T value;

  public SingleFieldPersistentDataset() {}

  public SingleFieldPersistentDataset(final String fieldName, final T value) {
    this();
    this.fieldName = fieldName;
    this.value = value;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.locationtech.geowave.core.store.data.PersistentDataSet#addValue(java.lang.String, T)
   */
  @Override
  public void addValue(final String fieldName, final T value) {
    this.fieldName = fieldName;
    this.value = value;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.locationtech.geowave.core.store.data.PersistentDataSet#addValues(java.util.Map)
   */
  @Override
  public void addValues(final Map<String, T> values) {
    if (!values.isEmpty()) {
      final Entry<String, T> e = values.entrySet().iterator().next();
      fieldName = e.getKey();
      value = e.getValue();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.locationtech.geowave.core.store.data.PersistentDataSet#getValue(java.lang.String)
   */
  @Override
  public T getValue(final String fieldName) {
    if ((this.fieldName == null) && (fieldName == null)) {
      return value;
    }
    if ((this.fieldName != null) && this.fieldName.equals(fieldName)) {
      return value;
    }
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.locationtech.geowave.core.store.data.PersistentDataSet#getValues()
   */
  @Override
  public Map<String, T> getValues() {
    return Collections.singletonMap(fieldName, value);
  }
}
