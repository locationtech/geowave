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
package org.locationtech.geowave.core.store.query.filter;

import java.util.List;

import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

/**
 * This class wraps a list of filters into a single filter such that if any one
 * filter fails this class will fail acceptance.
 * 
 * @param <T>
 */
public class FilterList<T extends QueryFilter> implements
		QueryFilter
{
	protected List<T> filters;
	protected boolean logicalAnd = true;

	protected FilterList() {}

	protected FilterList(
			boolean logicalAnd ) {
		this.logicalAnd = logicalAnd;
	}

	public FilterList(
			final List<T> filters ) {
		this.filters = filters;
	}

	public FilterList(
			boolean logicalAnd,
			final List<T> filters ) {
		this.logicalAnd = logicalAnd;
		this.filters = filters;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> entry ) {
		if (filters == null) return true;
		for (final QueryFilter filter : filters) {
			final boolean ok = filter.accept(
					indexModel,
					entry);
			if (!ok && logicalAnd) return false;
			if (ok && !logicalAnd) return true;

		}
		return logicalAnd;
	}

}
