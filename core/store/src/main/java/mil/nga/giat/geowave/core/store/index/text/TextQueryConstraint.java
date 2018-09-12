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
package mil.nga.giat.geowave.core.store.index.text;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;

/**
 * A class based on FilterableConstraints that uses a text value for query
 * 
 */

public class TextQueryConstraint implements
		FilterableConstraints
{
	private final ByteArrayId fieldId;
	private final String matchValue;
	private final boolean caseSensitive;

	public TextQueryConstraint(
			final ByteArrayId fieldId,
			final String matchValue,
			final boolean caseSensitive ) {
		super();
		this.fieldId = fieldId;
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
	public ByteArrayId getFieldId() {
		return fieldId;
	}

	@Override
	public DistributableQueryFilter getFilter() {
		return new TextExactMatchFilter(
				fieldId,
				matchValue,
				caseSensitive);
	}

	public QueryRanges getQueryRanges() {
		// TODO case sensitivity
		return new QueryRanges(
				new ByteArrayRange(
						new ByteArrayId(
								matchValue),
						new ByteArrayId(
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
