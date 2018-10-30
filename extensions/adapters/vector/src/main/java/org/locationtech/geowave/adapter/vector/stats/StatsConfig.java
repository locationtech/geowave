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
package org.locationtech.geowave.adapter.vector.stats;

import java.io.Serializable;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public interface StatsConfig<T> extends
		Serializable,
		Persistable
{
	InternalDataStatistics<T, ?, ?> create(
			Short internalDataAdapterId,
			final String fieldName );
}
