/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql.statement;

import org.locationtech.geowave.adapter.vector.query.gwql.ResultSet;
import org.locationtech.geowave.core.store.api.DataStore;

/**
 * Interface for GeoWave query language statements.
 */
public interface Statement {
  /**
   * Executes the statement on the provided data store.
   * 
   * @param dataStore the data store to execute the statement on
   * @return the results of the statement
   */
  public ResultSet execute(final DataStore dataStore);

  /**
   * @return the store name that this statement should be executed on
   */
  public String getStoreName();
}
