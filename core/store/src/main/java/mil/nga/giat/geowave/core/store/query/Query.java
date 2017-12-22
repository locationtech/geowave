/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store.query;

import java.util.List;

import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * This interface fully describes a query
 */
public interface Query
{
	/**
	 * This is a list of filters (either client filters or distributed filters)
	 * which will be applied to the result set. QueryFilters of type
	 * DistributableQueryFilter will automatically be distributed across nodes,
	 * although the class must be on the classpath of each node. Fine-grained
	 * filtering and secondary filtering should be applied here as the primary
	 * index will only enable coarse-grained filtering.
	 *
	 * @param indexModel
	 *            This can be used by the filters to determine the common fields
	 *            in the index
	 * @return A list of the query filters
	 */
	public List<QueryFilter> createFilters(
			PrimaryIndex index );

	/**
	 * This is useful to determine what indices this query supports. If an index
	 * is not supported the query will not be run on it and no data will be
	 * returned from that index.
	 *
	 * @param index
	 *            The index to check if the query will support.
	 * @return A flag indicating if this query supports the index
	 */
	public boolean isSupported(
			Index<?, ?> index );

	/**
	 * Return a set of constraints to apply to the primary index based on the
	 * indexing strategy used. The ordering of dimensions within the index
	 * stategy must match the order of dimensions in the numeric data returned
	 * which will represent the constraints applied to the primary index for the
	 * query.
	 *
	 * @param index
	 *            The index used to generate the constraints for
	 * @return A multi-dimensional numeric data set that represents the
	 *         constraints for the index
	 */
	public List<MultiDimensionalNumericData> getIndexConstraints(
			PrimaryIndex index );

}
