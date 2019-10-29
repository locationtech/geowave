package org.locationtech.geowave.adapter.vector.query.gwql;

/**
 * A combination of store name and type name used by queries to identify the data set to query.
 */
public class QualifiedTypeName {

  private final String storeName;
  private final String typeName;

  /**
   * @param typeName the type name to query
   */
  public QualifiedTypeName(final String typeName) {
    this(null, typeName);
  }

  /**
   * @param storeName the store to query
   * @param typeName the type name to query
   */
  public QualifiedTypeName(final String storeName, final String typeName) {
    this.storeName = storeName;
    this.typeName = typeName;
  }

  /**
   * @return the store name
   */
  public String storeName() {
    return storeName;
  }

  /**
   * @return the type name
   */
  public String typeName() {
    return typeName;
  }

}
