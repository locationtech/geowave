/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

public interface QueryIssuer {
  CloseableIterator<SimpleFeature> query(Index index, BasicQueryByClass constraints);

  Filter getFilter();

  Integer getLimit();
}
