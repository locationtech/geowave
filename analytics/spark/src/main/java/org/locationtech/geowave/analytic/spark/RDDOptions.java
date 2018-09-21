/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.analytic.spark;

import org.locationtech.geowave.core.store.api.QueryOptionsInt;
import org.locationtech.geowave.core.store.query.constraints.DistributableQuery;

public class RDDOptions
{
	private DistributableQuery query = null;
	private QueryOptionsInt queryOptions = null;
	private int minSplits = -1;
	private int maxSplits = -1;

	public RDDOptions() {}

	public DistributableQuery getQuery() {
		return query;
	}

	public void setQuery(
			DistributableQuery query ) {
		this.query = query;
	}

	public QueryOptionsInt getQueryOptions() {
		return queryOptions;
	}

	public void setQueryOptions(
			QueryOptionsInt queryOptions ) {
		this.queryOptions = queryOptions;
	}

	public int getMinSplits() {
		return minSplits;
	}

	public void setMinSplits(
			int minSplits ) {
		this.minSplits = minSplits;
	}

	public int getMaxSplits() {
		return maxSplits;
	}

	public void setMaxSplits(
			int maxSplits ) {
		this.maxSplits = maxSplits;
	}

}
