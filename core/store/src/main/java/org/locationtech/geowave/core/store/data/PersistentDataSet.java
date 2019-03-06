package org.locationtech.geowave.core.store.data;

import java.util.Map;


public interface PersistentDataSet<T> {

  /**
   * Add the field ID/value pair to this data set. Do not overwrite.
   *
   * @param value the field ID/value pair to add
   */
  void addValue(String fieldName, T value);

  /** Add several values to the data set. */
  void addValues(Map<String, T> values);

  /**
   * Given a field ID, get the associated value
   *
   * @param fieldName the field ID
   * @return the stored field value, null if this does not contain a value for the ID
   */
  T getValue(String fieldName);

  /**
   * Get all of the values from this persistent data set
   *
   * @return all of the value
   */
  Map<String, T> getValues();
}
