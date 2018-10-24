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
package org.locationtech.geowave.core.geotime.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.index.FilterableConstraints;
import org.locationtech.geowave.core.store.index.SecondaryIndexImpl;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class PropertyConstraintSet
{
	private final Map<String, FilterableConstraints> constraints = new HashMap<>();

	public PropertyConstraintSet() {}

	public PropertyConstraintSet(
			final FilterableConstraints constraint ) {
		add(
				constraint,
				true);
	}

	public boolean isEmpty() {
		return constraints.isEmpty();
	}

	public List<FilterableConstraints> getConstraintsFor(
			final String[] fieldIds ) {
		final List<FilterableConstraints> result = new LinkedList<>();
		for (final String fieldId : fieldIds) {
			final FilterableConstraints c = constraints.get(fieldId);
			if (c != null) {
				result.add(c);
			}

		}
		return result;
	}

	public List<ByteArrayRange> getRangesFor(
			final SecondaryIndexImpl<?> index ) {
		final List<ByteArrayRange> result = new LinkedList<>();
		final FilterableConstraints c = constraints.get(index.getFieldName());
		if (c != null) {
			// TODO GEOWAVE-1018 how to handle secondary index ranges?
			result.addAll(index.getIndexStrategy().getQueryRanges(
					c).getCompositeQueryRanges());
		}
		return result;
	}

	public List<QueryFilter> getFiltersFor(
			final SecondaryIndexImpl<?> index ) {
		final List<QueryFilter> result = new LinkedList<>();
		final FilterableConstraints c = constraints.get(index.getFieldName());
		if (c != null) {
			final QueryFilter filter = c.getFilter();
			if (filter != null) {
				result.add(filter);
			}
		}
		return result;
	}

	public void add(
			final FilterableConstraints constraint,
			final boolean intersect ) {
		final String id = constraint.getFieldName();
		final FilterableConstraints constraintsForId = constraints.get(id);
		if (constraintsForId == null) {
			constraints.put(
					id,
					constraint);
		}
		else if (intersect) {
			constraints.put(
					id,
					constraintsForId.intersect(constraint));
		}
		else {
			constraints.put(
					id,
					constraintsForId.union(constraint));
		}
	}

	public void intersect(
			final PropertyConstraintSet set ) {
		for (final Map.Entry<String, FilterableConstraints> entry : set.constraints.entrySet()) {
			add(
					entry.getValue(),
					true);
		}
	}

	public void union(
			final PropertyConstraintSet set ) {
		for (final Map.Entry<String, FilterableConstraints> entry : set.constraints.entrySet()) {
			add(
					entry.getValue(),
					false);
		}
	}

	public FilterableConstraints getConstraintsByName(
			final String id ) {
		return constraints.get(id);
	}

}
