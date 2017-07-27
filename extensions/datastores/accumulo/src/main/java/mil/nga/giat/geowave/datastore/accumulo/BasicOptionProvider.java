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
package mil.nga.giat.geowave.datastore.accumulo;

import java.util.Map;

import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig.OptionProvider;

public class BasicOptionProvider implements
		OptionProvider
{
	private final Map<String, String> options;

	public BasicOptionProvider(
			final Map<String, String> options ) {
		this.options = options;
	}

	@Override
	public Map<String, String> getOptions(
			final Map<String, String> existingOptions ) {
		return options;
	}

}
