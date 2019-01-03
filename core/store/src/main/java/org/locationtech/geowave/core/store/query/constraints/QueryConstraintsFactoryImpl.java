/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership. All rights reserved. This program and the accompanying materials are made available under the terms of the Apache License, Version 2.0 which accompanies this distribution and is available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.store.api.QueryConstraintsFactory;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;

public class QueryConstraintsFactoryImpl implements
		QueryConstraintsFactory
{
	public static final QueryConstraintsFactoryImpl SINGLETON_INSTANCE = new QueryConstraintsFactoryImpl();

	@Override
	public QueryConstraints dataIds(
			final ByteArray[] dataIds ) {
		return new DataIdQuery(
				dataIds);
	}

	@Override
	public QueryConstraints prefix(
			final ByteArray partitionKey,
			final ByteArray sortKeyPrefix ) {
		return new PrefixIdQuery(
				partitionKey,
				sortKeyPrefix);
	}

	@Override
	public QueryConstraints coordinateRanges(
			final NumericIndexStrategy indexStrategy,
			final MultiDimensionalCoordinateRangesArray[] coordinateRanges ) {
		return new CoordinateRangeQuery(
				indexStrategy,
				coordinateRanges);
	}

	@Override
	public QueryConstraints constraints(
			final Constraints constraints ) {
		return new BasicQuery(
				constraints);
	}

	@Override
	public QueryConstraints constraints(
			final Constraints constraints,
			final BasicQueryCompareOperation compareOp ) {
		return new BasicQuery(
				constraints,
				compareOp);
	}

	@Override
	public QueryConstraints noConstraints() {
		return new EverythingQuery();
	}

}
