/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query;

import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geowave.core.geotime.store.query.api.SpatialTemporalConstraintsBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryConstraintsFactory;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraintsFactoryImpl;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorQueryConstraintsFactoryImpl extends QueryConstraintsFactoryImpl implements
    VectorQueryConstraintsFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(OptimalCQLQuery.class);

  public static final VectorQueryConstraintsFactoryImpl SINGLETON_INSTANCE =
      new VectorQueryConstraintsFactoryImpl();

  @Override
  public SpatialTemporalConstraintsBuilder spatialTemporalConstraints() {
    return new SpatialTemporalConstraintsBuilderImpl();
  }

  // these cql expressions should always attempt to use
  // CQLQuery.createOptimalQuery() which requires adapter and index
  @Override
  public QueryConstraints cqlConstraints(final String cqlExpression) {
    GeometryUtils.initClassLoader();
    try {
      final Filter cqlFilter = ECQL.toFilter(cqlExpression);
      return new OptimalCQLQuery(cqlFilter);
    } catch (final CQLException e) {
      LOGGER.error("Unable to parse CQL expresion", e);
    }
    return null;
  }

  @Override
  public QueryConstraints filterConstraints(final Filter filter) {
    return new OptimalCQLQuery(filter);
  }
}
