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

import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;

/**
 * This interface fully describes a query and is persistable so that it can be
 * distributed if necessary (particularly useful for using a query as mapreduce
 * input)
 */
public interface DistributableQuery extends
		Query,
		Persistable
{
	/**
	 * Return a set of constraints to apply to the given secondary index.
	 * 
	 * @param index
	 *            the index to extract constraints for
	 * @return A collection of ranges over secondary index keys.
	 */
	public List<ByteArrayRange> getSecondaryIndexConstraints(
			SecondaryIndex<?> index );

	public List<DistributableQueryFilter> getSecondaryQueryFilter(
			SecondaryIndex<?> index );
}
