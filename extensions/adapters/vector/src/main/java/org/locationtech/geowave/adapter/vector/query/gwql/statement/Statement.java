package org.locationtech.geowave.adapter.vector.query.gwql.statement;

import org.locationtech.geowave.adapter.vector.query.gwql.ResultSet;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;

/**
 * Interface for GeoWave query language statements.
 */
public interface Statement {
  /**
   * Executes the statement on the provided data store.
   * 
   * @param storeOptions the data store plugin options
   * @return the results of the statement
   */
  public ResultSet execute(final DataStorePluginOptions storeOptions);

  /**
   * @return the store name that this statement should be executed on
   */
  public String getStoreName();
}
