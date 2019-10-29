package org.locationtech.geowave.adapter.vector.query.gwql.function;

/**
 * Base interface for all functions in the query language.
 */
public interface QLFunction {
  public Class<?> returnType();
}
