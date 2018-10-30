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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.locationtech.geowave.core.index.IndexConstraints;
import org.locationtech.geowave.core.store.query.filter.FilterList;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class CompositeConstraints implements
		FilterableConstraints
{
	private final List<FilterableConstraints> constraints = new LinkedList<FilterableConstraints>();
	private boolean intersect = false;

	public CompositeConstraints() {}

	public CompositeConstraints(
			final List<FilterableConstraints> constraints ) {
		super();
		this.constraints.addAll(constraints);
	}

	public CompositeConstraints(
			final List<FilterableConstraints> constraints,
			final boolean intersect ) {
		super();
		this.constraints.addAll(constraints);
		this.intersect = intersect;
	}

	public List<FilterableConstraints> getConstraints() {
		return constraints;
	}

	@Override
	public int getDimensionCount() {
		return constraints == null ? 0 : constraints.size();
	}

	@Override
	public boolean isEmpty() {
		return (constraints == null) || constraints.isEmpty();
	}

	@Override
	public QueryFilter getFilter() {
		final List<QueryFilter> filters = new ArrayList<QueryFilter>();
		for (final IndexConstraints constraint : constraints) {
			if (constraint instanceof FilterableConstraints) {
				QueryFilter filter = ((FilterableConstraints) constraint).getFilter();
				if (filter != null) {
					filters.add(filter);
				}
			}
		}
		if (filters.isEmpty()) {
			return null;
		}
		if (filters.size() == 1) {
			return filters.get(0);
		}
		return new FilterList(
				intersect,
				filters);
	}

	@Override
	public String getFieldName() {
		return constraints.get(
				0).getFieldName();
	}

	@Override
	public FilterableConstraints intersect(
			final FilterableConstraints constraints ) {
		final CompositeConstraints cc = new CompositeConstraints(
				this.constraints,
				true);
		cc.constraints.add(constraints);
		return cc;
	}

	@Override
	public FilterableConstraints union(
			final FilterableConstraints constraints ) {
		final CompositeConstraints cc = new CompositeConstraints(
				this.constraints);
		cc.constraints.add(constraints);
		return cc;
	}

}
