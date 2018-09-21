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
package org.locationtech.geowave.core.geotime.util;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.opengis.feature.simple.SimpleFeatureType;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * 
 * A type of configuration data associated with attributes of a simple features
 * such as statistics, indexing constraints, etc.
 * 
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public interface SimpleFeatureUserDataConfiguration extends
		java.io.Serializable,
		Persistable
{
	/**
	 * Store configuration in user data of the feature type attributes.
	 * 
	 * @param type
	 */
	public void updateType(
			final SimpleFeatureType type );

	/**
	 * Extract configuration from user data of the feature type attributes.
	 * 
	 * @param type
	 */
	public void configureFromType(
			final SimpleFeatureType type );
}
