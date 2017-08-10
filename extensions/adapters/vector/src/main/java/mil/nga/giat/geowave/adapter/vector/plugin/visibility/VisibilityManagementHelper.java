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
package mil.nga.giat.geowave.adapter.vector.plugin.visibility;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.SPIServiceRegistry;

/**
 * At the moment, the expectation is that a single GeoServer instance supports
 * only one visibility management approach/format.
 * 
 * 
 * 
 * 
 */
public class VisibilityManagementHelper
{

	protected final static Logger LOGGER = LoggerFactory.getLogger(VisibilityManagementHelper.class);

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static final <T> ColumnVisibilityManagementSpi<T> loadVisibilityManagement() {
		Iterator<ColumnVisibilityManagementSpi> managers = new SPIServiceRegistry(
				VisibilityManagementHelper.class).load(ColumnVisibilityManagementSpi.class);
		if (!managers.hasNext()) return new JsonDefinitionColumnVisibilityManagement<T>();
		return (ColumnVisibilityManagementSpi<T>) managers.next();
	}
}
