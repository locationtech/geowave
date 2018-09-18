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
package org.locationtech.geowave.format.geotools.vector;

import java.util.ArrayList;
import java.util.List;

import org.locationtech.geowave.adapter.vector.ingest.CQLFilterOptionProvider;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;
import org.locationtech.geowave.format.geotools.vector.retyping.date.DateFieldOptionProvider;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class GeoToolsVectorDataOptions implements
		IngestFormatOptions
{

	@ParametersDelegate
	private CQLFilterOptionProvider cqlFilterOptionProvider = new CQLFilterOptionProvider();

	@Parameter(names = "--type", description = "Optional parameter that specifies specific type name(s) from the source file", required = false)
	private List<String> featureTypeNames = new ArrayList<>();

	@ParametersDelegate
	private DateFieldOptionProvider dateFieldOptionProvider = new DateFieldOptionProvider();

	public GeoToolsVectorDataOptions() {}

	public CQLFilterOptionProvider getCqlFilterOptionProvider() {
		return cqlFilterOptionProvider;
	}

	public void setCqlFilterOptionProvider(
			CQLFilterOptionProvider cqlFilterOptionProvider ) {
		this.cqlFilterOptionProvider = cqlFilterOptionProvider;
	}

	public DateFieldOptionProvider getDateFieldOptionProvider() {
		return dateFieldOptionProvider;
	}

	public void setDateFieldOptionProvider(
			DateFieldOptionProvider dateFieldOptionProvider ) {
		this.dateFieldOptionProvider = dateFieldOptionProvider;
	}

	public List<String> getFeatureTypeNames() {
		return featureTypeNames;
	}

	public void setFeatureTypeNames(
			List<String> featureTypeNames ) {
		this.featureTypeNames = featureTypeNames;
	}
}
