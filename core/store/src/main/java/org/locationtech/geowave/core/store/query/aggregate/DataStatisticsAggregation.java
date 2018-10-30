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
package org.locationtech.geowave.core.store.query.aggregate;

import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.api.Aggregation;

public class DataStatisticsAggregation<T> implements
		Aggregation<InternalDataStatistics<T, ?, ?>, InternalDataStatistics<T, ?, ?>, T>
{
	private InternalDataStatistics<T, ?, ?> statisticsParam;

	private InternalDataStatistics<T, ?, ?> statisticsResult;
	private byte[] defaultResultBinary;

	public DataStatisticsAggregation() {}

	public DataStatisticsAggregation(
			final InternalDataStatistics<T, ?, ?> statistics ) {
		this.statisticsResult = statistics;
		this.defaultResultBinary = PersistenceUtils.toBinary(statisticsResult);
		this.statisticsParam = statistics;
	}

	@Override
	public void aggregate(
			final T entry ) {
		statisticsResult.entryIngested(entry);
	}

	@Override
	public InternalDataStatistics<T, ?, ?> getParameters() {
		return statisticsParam;
	}

	@Override
	public void setParameters(
			final InternalDataStatistics<T, ?, ?> parameters ) {
		this.statisticsParam = parameters;
	}

	@Override
	public void clearResult() {
		this.statisticsResult = (InternalDataStatistics<T, ?, ?>) PersistenceUtils.fromBinary(defaultResultBinary);
	}

	@Override
	public InternalDataStatistics<T, ?, ?> getResult() {
		return statisticsResult;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public byte[] resultToBinary(
			final InternalDataStatistics<T, ?, ?> result ) {
		return PersistenceUtils.toBinary(result);
	}

	@Override
	public InternalDataStatistics<T, ?, ?> resultFromBinary(
			final byte[] binary ) {
		return (InternalDataStatistics<T, ?, ?>) PersistenceUtils.fromBinary(binary);
	}

}
