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
package mil.nga.giat.geowave.adapter.vector.stats;

import java.io.Serializable;

import org.codehaus.jackson.annotate.JsonTypeInfo;

import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public interface StatsConfig<T> extends
		Serializable
{
	DataStatistics<T> create(
			Short internalDataAdapterId,
			final String fieldName );
}
