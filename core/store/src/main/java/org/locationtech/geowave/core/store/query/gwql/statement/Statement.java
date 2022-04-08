/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql.statement;

import org.locationtech.geowave.core.store.query.gwql.ResultSet;

/**
 * Interface for GeoWave query language statements.
 */
public interface Statement {
  /**
   * Executes the statement with the provided authorizations.
   *
   * @param authorizations authorizations to use for the query
   * @return the results of the statement
   */
  public ResultSet execute(final String... authorizations);
}
