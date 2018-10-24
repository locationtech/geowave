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

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;
import org.locationtech.geowave.core.store.callback.IngestCallback;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public interface InternalDataStatistics<T, R, B extends StatisticsQueryBuilder<R, B>> extends
		Mergeable,
		IngestCallback<T>
{
	Short getAdapterId();

	R getResult();

	StatisticsType<R, B> getType();

	String getExtendedId();

	void setAdapterId(
			short dataAdapterId );

	void setType(
			StatisticsType<R, B> statisticsId );

	void setExtendedId(
			String id );

	void setVisibility(
			byte[] visibility );

	byte[] getVisibility();

	JSONObject toJSONObject(
			InternalAdapterStore adapterStore )
			throws JSONException;

	InternalDataStatistics<T, R, B> duplicate();

	static < R> InternalDataStatistics<?, R, ?> reduce(
			final InternalDataStatistics<?, R, ?> a,
			final InternalDataStatistics<?, R, ?> b ) {
		a
				.merge(
						b);
		return a;
	}
}
