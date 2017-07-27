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
package mil.nga.giat.geowave.format.geotools.vector;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.adapter.vector.ingest.CQLFilterOptionProvider;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;
import mil.nga.giat.geowave.format.geotools.vector.retyping.date.DateFieldOptionProvider;

public class GeoToolsVectorDataOptions implements
		IngestFormatOptionProvider
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
