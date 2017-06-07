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
package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;

/**
 * This interface defines the set of statistics to capture for a specific
 * adapter.
 * 
 * @param <T>
 *            The type for the data elements that are being adapted by the
 *            adapter
 * 
 */
public interface StatisticsProvider<T>
{
	public ByteArrayId[] getSupportedStatisticsTypes();

	public DataStatistics<T> createDataStatistics(
			ByteArrayId statisticsId );

	public EntryVisibilityHandler<T> getVisibilityHandler(
			ByteArrayId statisticsId );
}
