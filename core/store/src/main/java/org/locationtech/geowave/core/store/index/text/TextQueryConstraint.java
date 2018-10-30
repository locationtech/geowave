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
package org.locationtech.geowave.core.store.index.text;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.store.index.FilterableConstraints;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

/**
 * A class based on FilterableConstraints that uses a text value for query
 *
 */

public class TextQueryConstraint implements
		FilterableConstraints
{
	private final String fieldName;
	private final String matchValue;
	private final boolean caseSensitive;

	public TextQueryConstraint(
			final String fieldName,
			final String matchValue,
			final boolean caseSensitive ) {
		super();
		this.fieldName = fieldName;
		this.matchValue = matchValue;
		this.caseSensitive = caseSensitive;
	}

	@Override
	public int getDimensionCount() {
		return 1;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public String getFieldName() {
		return fieldName;
	}

	@Override
	public QueryFilter getFilter() {
		return new TextExactMatchFilter(
				fieldName,
				matchValue,
				caseSensitive);
	}

	public QueryRanges getQueryRanges() {
		// TODO case sensitivity
		return new QueryRanges(
				new ByteArrayRange(
						new ByteArray(
								matchValue),
						new ByteArray(
								matchValue)));
	}

	@Override
	public FilterableConstraints intersect(
			final FilterableConstraints constaints ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FilterableConstraints union(
			final FilterableConstraints constaints ) {
		// TODO Auto-generated method stub
		return null;
	}

}
