/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index;

import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.persist.Persistable;

/**
 * This interface is the most straightforward mechanism to add custom indexing of any arbitrary
 * logic to a GeoWave data store. This can simply be two functions that tell GeoWave how to index an
 * entry on ingest and how to query the index based on a custom constraints type.
 * 
 * @param <E> The entry type (such as SimpleFeature, GridCoverage, or whatever type the adapter
 *        uses)
 * @param <C> The custom constraints type, can be any arbitrary type, although should be persistable
 *        so that it can work outside of just client code (such as server-side filtering,
 *        map-reduce, or spark)
 */
public interface CustomIndexStrategy<E, C extends Persistable> extends Persistable {
  /**
   * This is the function that is called on ingest to tell GeoWave how to index the entry within
   * this custom index - the insertion IDs are a set of partition and sort keys, either of which
   * could be empty or null as needed (with the understanding that each partition key represents a
   * unique partition in the backend datastore)
   * 
   * @param entry the entry to be indexed on ingest
   * @return the insertion IDs representing how to index the entry
   */
  InsertionIds getInsertionIds(E entry);

  /**
   * This is the function that is called on query, when given a query with the constraints type. The
   * constraints type can be any arbtitrary type although should be persistable so that it can work
   * outside of just client code (such as server-side filtering, map-reduce, or spark).
   *
   * The query ranges are a set of partition keys and ranges of sort keys that fully include all
   * rows that may match the constraints.
   * 
   * @param constraints the query constraints
   * @return query ranges that represent valid partition and ranges of sort keys that fully include
   *         all rows that may match the constraints
   */
  QueryRanges getQueryRanges(C constraints);
}
