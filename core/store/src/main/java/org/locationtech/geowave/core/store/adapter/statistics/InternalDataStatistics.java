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

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.api.Statistics;
import org.locationtech.geowave.core.store.callback.IngestCallback;

public interface InternalDataStatistics<T, R> extends
		Mergeable,
		IngestCallback<T>,
		Statistics<R>
{
	Short getInternalDataAdapterId();

	void setInternalDataAdapterId(
			short dataAdapterId );

	void setStatisticsType(
			StatisticsType<R> statisticsId );

	void setVisibility(
			byte[] visibility );

	byte[] getVisibility();

	JSONObject toJSONObject(
			InternalAdapterStore adapterStore )
			throws JSONException;
}
