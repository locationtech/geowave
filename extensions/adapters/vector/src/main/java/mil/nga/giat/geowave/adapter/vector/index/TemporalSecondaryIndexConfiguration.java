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
package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;

import org.codehaus.jackson.annotate.JsonIgnore;

public class TemporalSecondaryIndexConfiguration extends
		AbstractSecondaryIndexConfiguration<Date>
{

	private static final long serialVersionUID = -4061430414572409815L;
	public static final String INDEX_KEY = "2ND_IDX_TEMPORAL";

	public TemporalSecondaryIndexConfiguration() {
		super(
				Date.class,
				Collections.<String> emptySet(),
				SecondaryIndexType.JOIN);
	}

	public TemporalSecondaryIndexConfiguration(
			final String attribute,
			final SecondaryIndexType secondaryIndexType ) {
		super(
				Date.class,
				attribute,
				secondaryIndexType);
	}

	public TemporalSecondaryIndexConfiguration(
			final Set<String> attributes,
			final SecondaryIndexType secondaryIndexType ) {
		super(
				Date.class,
				attributes,
				secondaryIndexType);
	}

	public TemporalSecondaryIndexConfiguration(
			final String attribute,
			final SecondaryIndexType secondaryIndexType,
			final List<String> fieldIds ) {
		super(
				Date.class,
				attribute,
				secondaryIndexType,
				fieldIds);
	}

	public TemporalSecondaryIndexConfiguration(
			final Set<String> attributes,
			final SecondaryIndexType secondaryIndexType,
			final List<String> fieldIds ) {
		super(
				Date.class,
				attributes,
				secondaryIndexType,
				fieldIds);
	}

	@JsonIgnore
	@Override
	public String getIndexKey() {
		return INDEX_KEY;
	}

}
