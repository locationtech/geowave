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
package org.locationtech.geowave.core.store.adapter.statistics;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.CloseableIterator;

/**
 * This is responsible for persisting data statistics (either in memory or to
 * disk depending on the implementation).
 */
public interface DataStatisticsStore
{
	/**
	 * This will write the statistics to the underlying store. Note that this
	 * will overwrite whatever the current persisted statistics are with the
	 * given statistics ID and data adapter ID. Use incorporateStatistics to
	 * aggregate the statistics with any existing statistics.
	 * 
	 * @param statistics
	 *            The statistics to write
	 * 
	 */
	public void setStatistics(
			InternalDataStatistics<?> statistics );

	/**
	 * Add the statistics to the store, overwriting existing data statistics
	 * with the aggregation of these statistics and the existing statistics
	 * 
	 * @param statistics
	 *            the data statistics
	 */
	public void incorporateStatistics(
			InternalDataStatistics<?> statistics );

	/**
	 * Get all data statistics from the store by a data adapter ID
	 * 
	 * @param adapterId
	 *            the data adapter ID
	 * @return the list of statistics for the given adapter, empty if it doesn't
	 *         exist
	 */
	public CloseableIterator<InternalDataStatistics<?>> getDataStatistics(
			short internalAdapterId,
			String... authorizations );

	/**
	 * Get all data statistics from the store
	 * 
	 * @return the list of all statistics
	 */
	public CloseableIterator<InternalDataStatistics<?>> getAllDataStatistics(
			String... authorizations );

	/**
	 * Get statistics by adapter ID and the statistics ID (which will define a
	 * unique statistic)
	 * 
	 * @param adapterId
	 *            The adapter ID for the requested statistics
	 * @param statisticsId
	 *            the statistics ID for the requested statistics
	 * @return the persisted statistics value
	 */
	public InternalDataStatistics<?> getDataStatistics(
			short internalAdapterId,
			ByteArrayId statisticsId,
			String... authorizations );

	/**
	 * Remove a statistic from the store
	 * 
	 * @param adapterId
	 * @param statisticsId
	 * @return a flag indicating whether a statistic had existed with the given
	 *         IDs and was successfully deleted.
	 */
	public boolean removeStatistics(
			short internalAdapterId,
			ByteArrayId statisticsId,
			String... authorizations );

	/**
	 * Remove all statistics with a given adapter ID from the store
	 * 
	 * @param adapterId
	 */
	public void removeAllStatistics(
			short internalAdapterId,
			String... authorizations );

	public void removeAll();
}
