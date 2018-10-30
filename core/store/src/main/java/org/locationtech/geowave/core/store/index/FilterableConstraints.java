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
package org.locationtech.geowave.core.store.index;

import org.locationtech.geowave.core.index.IndexConstraints;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public interface FilterableConstraints extends
		IndexConstraints
{

	public String getFieldName();

	public QueryFilter getFilter();

	public FilterableConstraints intersect(
			FilterableConstraints constaints );

	public FilterableConstraints union(
			FilterableConstraints constaints );
}
