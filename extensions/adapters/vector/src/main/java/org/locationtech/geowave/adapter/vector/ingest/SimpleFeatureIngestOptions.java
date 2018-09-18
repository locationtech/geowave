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
package org.locationtech.geowave.adapter.vector.ingest;

import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;

import com.beust.jcommander.ParametersDelegate;

/**
 * This class is a holder class for options used in AbstractSimpleFeatureIngest.
 */
public class SimpleFeatureIngestOptions implements
		IngestFormatOptions
{

	@ParametersDelegate
	private CQLFilterOptionProvider cqlFilterOptionProvider = new CQLFilterOptionProvider();

	@ParametersDelegate
	private TypeNameOptionProvider typeNameOptionProvider = new TypeNameOptionProvider();

	@ParametersDelegate
	private FeatureSerializationOptionProvider serializationFormatOptionProvider = new FeatureSerializationOptionProvider();

	@ParametersDelegate
	private GeometrySimpOptionProvider simpOptionProvider = new GeometrySimpOptionProvider();

	@ParametersDelegate
	private Object pluginOptions = null;

	public SimpleFeatureIngestOptions() {}

	public GeometrySimpOptionProvider getGeometrySimpOptionProvider() {
		return simpOptionProvider;
	}

	public void setGeometrySimpOptionProvider(
			final GeometrySimpOptionProvider simpOptionProvider ) {
		this.simpOptionProvider = simpOptionProvider;
	}

	public CQLFilterOptionProvider getCqlFilterOptionProvider() {
		return cqlFilterOptionProvider;
	}

	public void setCqlFilterOptionProvider(
			final CQLFilterOptionProvider cqlFilterOptionProvider ) {
		this.cqlFilterOptionProvider = cqlFilterOptionProvider;
	}

	public TypeNameOptionProvider getTypeNameOptionProvider() {
		return typeNameOptionProvider;
	}

	public void setTypeNameOptionProvider(
			final TypeNameOptionProvider typeNameOptionProvider ) {
		this.typeNameOptionProvider = typeNameOptionProvider;
	}

	public FeatureSerializationOptionProvider getSerializationFormatOptionProvider() {
		return serializationFormatOptionProvider;
	}

	public void setSerializationFormatOptionProvider(
			final FeatureSerializationOptionProvider serializationFormatOptionProvider ) {
		this.serializationFormatOptionProvider = serializationFormatOptionProvider;
	}

	public Object getPluginOptions() {
		return pluginOptions;
	}

	public void setPluginOptions(
			final Object pluginOptions ) {
		this.pluginOptions = pluginOptions;
	}
}
