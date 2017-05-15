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
package mil.nga.giat.geowave.adapter.vector.ingest;

import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;

/**
 * This class is a holder class for options used in AbstractSimpleFeatureIngest.
 */
public class SimpleFeatureIngestOptions implements
		IngestFormatOptionProvider
{

	@ParametersDelegate
	private CQLFilterOptionProvider cqlFilterOptionProvider = new CQLFilterOptionProvider();

	@ParametersDelegate
	private TypeNameOptionProvider typeNameOptionProvider = new TypeNameOptionProvider();

	@ParametersDelegate
	private FeatureSerializationOptionProvider serializationFormatOptionProvider = new FeatureSerializationOptionProvider();

	@ParametersDelegate
	private Object pluginOptions = null;

	public SimpleFeatureIngestOptions() {}

	public CQLFilterOptionProvider getCqlFilterOptionProvider() {
		return cqlFilterOptionProvider;
	}

	public void setCqlFilterOptionProvider(
			CQLFilterOptionProvider cqlFilterOptionProvider ) {
		this.cqlFilterOptionProvider = cqlFilterOptionProvider;
	}

	public TypeNameOptionProvider getTypeNameOptionProvider() {
		return typeNameOptionProvider;
	}

	public void setTypeNameOptionProvider(
			TypeNameOptionProvider typeNameOptionProvider ) {
		this.typeNameOptionProvider = typeNameOptionProvider;
	}

	public FeatureSerializationOptionProvider getSerializationFormatOptionProvider() {
		return serializationFormatOptionProvider;
	}

	public void setSerializationFormatOptionProvider(
			FeatureSerializationOptionProvider serializationFormatOptionProvider ) {
		this.serializationFormatOptionProvider = serializationFormatOptionProvider;
	}

	public Object getPluginOptions() {
		return pluginOptions;
	}

	public void setPluginOptions(
			Object pluginOptions ) {
		this.pluginOptions = pluginOptions;
	}
}
