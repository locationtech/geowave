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
package org.locationtech.geowave.core.geotime.store;

import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsProvider;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public interface GeotoolsFeatureDataAdapter extends
		DataTypeAdapter<SimpleFeature>,
		StatisticsProvider<SimpleFeature>
{
	public SimpleFeatureType getFeatureType();

	public TimeDescriptors getTimeDescriptors();

	public boolean hasTemporalConstraints();

}
