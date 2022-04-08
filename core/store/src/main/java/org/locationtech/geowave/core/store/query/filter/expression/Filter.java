/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;

/**
 * Base interface for GeoWave filter objects. These filters can be used to generate constraints and
 * test entries of a data type adapter to see if they match a set of conditions.
 */
public interface Filter extends Persistable {

  /**
   * Evaluate this filter using a map of field values.
   * 
   * @param fieldValues the field values to evaluate the expression with, the key represents the
   *        field name, and the value represents the field value
   * @return {@code true} if the filter passes
   */
  boolean evaluate(Map<String, Object> fieldValues);

  /**
   * Evaluate this filter using the given adapter and entry.
   * 
   * @param <T> the class of the adapter entries
   * @param adapter the data type adapter
   * @param entry the entry to test
   * @return {@code true} if the filter passes
   */
  <T> boolean evaluate(DataTypeAdapter<T> adapter, T entry);

  /**
   * Prepare this filter for efficient testing using the provided adapter and index.
   * 
   * @param adapter the data type adapter
   * @param indexMapping the adapter to index mapping
   * @param index the index
   */
  void prepare(DataTypeAdapter<?> adapter, AdapterToIndexMapping indexMapping, Index index);

  /**
   * Adds all adapter fields referenced by this filter to the provided set.
   * 
   * @param fields the set to populate with the referenced fields
   */
  void addReferencedFields(Set<String> fields);

  /**
   * @return a set of all fields that can potentially be constrained by the filter
   */
  Set<String> getConstrainableFields();

  /**
   * Remove any exact and constrained predicates that reference fields in the provided set.
   * 
   * @param fields the fields to remove
   * @return an updated filter with the predicates removed
   */
  Filter removePredicatesForFields(Set<String> fields);

  /**
   * Generate constraints for the given index based on this filter.
   * 
   * @param constraintClass the class that the index expects for constraints
   * @param statsStore the data statistics store
   * @param adapter the data type adapter
   * @param indexMapping the adapter to index mapping
   * @param index the index
   * @param indexedFields a set of all adapter fields used by the index mapping
   * @return the constraints for the index that this filter represents
   */
  default <V extends Comparable<V>> FilterConstraints<V> getConstraints(
      final Class<V> constraintClass,
      final DataStatisticsStore statsStore,
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final Set<String> indexedFields) {
    return FilterConstraints.empty();
  }

  /**
   * Combine this filter with one or more other filters using an AND operator.
   * 
   * @param other the other filters to combine this one with
   * @return the combined filter
   */
  default Filter and(final Filter... other) {
    final Filter[] filters = new Filter[other.length + 1];
    filters[0] = this;
    System.arraycopy(other, 0, filters, 1, other.length);
    return new And(filters);
  }

  /**
   * Combine this filter with one or more other filters using an OR operator.
   * 
   * @param other the other filters to combine this one with
   * @return the combined filter
   */
  default Filter or(final Filter... other) {
    final Filter[] filters = new Filter[other.length + 1];
    filters[0] = this;
    System.arraycopy(other, 0, filters, 1, other.length);
    return new Or(filters);
  }

  /**
   * Create the inverse filter for the provided filter.
   * 
   * @param filter the filter to invert
   * @return the inverted filter
   */
  public static Filter not(final Filter filter) {
    return new Not(filter);
  }

  /**
   * Create a filter that always evaluates to {@code true}
   * 
   * @return the include filter
   */
  public static Filter include() {
    return new Include();
  }

  /**
   * Create a filter that always evaluates to {@code false}
   * 
   * @return the exclude filter
   */
  public static Filter exclude() {
    return new Exclude();
  }

}
