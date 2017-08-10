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
package mil.nga.giat.geowave.core.store.query.aggregate;

import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

public class DataStatisticsAggregation<T> implements
		Aggregation<DataStatistics<T>, DataStatistics<T>, T>
{
	private DataStatistics<T> statisticsParam;

	private DataStatistics<T> statisticsResult;
	private byte[] defaultResultBinary;

	public DataStatisticsAggregation() {}

	public DataStatisticsAggregation(
			final DataStatistics<T> statistics ) {
		this.statisticsResult = statistics;
		this.defaultResultBinary = PersistenceUtils.toBinary(statisticsResult);
		this.statisticsParam = statistics;
	}

	@Override
	public void aggregate(
			final T entry ) {
		statisticsResult.entryIngested(
				null,
				entry);
	}

	@Override
	public DataStatistics<T> getParameters() {
		return statisticsParam;
	}

	@Override
	public void setParameters(
			final DataStatistics<T> parameters ) {
		this.statisticsParam = parameters;
	}

	@Override
	public void clearResult() {
		this.statisticsResult = (DataStatistics<T>) PersistenceUtils.fromBinary(defaultResultBinary);
	}

	@Override
	public DataStatistics<T> getResult() {
		return statisticsResult;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

}
