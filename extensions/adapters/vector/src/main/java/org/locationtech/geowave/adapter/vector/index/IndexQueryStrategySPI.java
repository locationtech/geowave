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
package org.locationtech.geowave.adapter.vector.index;

import java.util.Map;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery;
import org.opengis.feature.simple.SimpleFeature;

public interface IndexQueryStrategySPI
{
	public enum QueryHint {
		MAX_RANGE_DECOMPOSITION
	}

	public CloseableIterator<Index> getIndices(
			Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> stats,
			BasicQuery query,
			Index[] indices,
			Map<QueryHint, Object> hints );
}
