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

import java.util.EnumSet;
import java.util.Map;

import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

public class IteratorConfig
{
	private final EnumSet<IteratorScope> scopes;
	private final int iteratorPriority;
	private final String iteratorName;
	private final String iteratorClass;
	private final OptionProvider optionProvider;

	public IteratorConfig(
			final EnumSet<IteratorScope> scopes,
			final int iteratorPriority,
			final String iteratorName,
			final String iteratorClass,
			final OptionProvider optionProvider ) {
		this.scopes = scopes;
		this.iteratorPriority = iteratorPriority;
		this.iteratorName = iteratorName;
		this.iteratorClass = iteratorClass;
		this.optionProvider = optionProvider;
	}

	public EnumSet<IteratorScope> getScopes() {
		return scopes;
	}

	public int getIteratorPriority() {
		return iteratorPriority;
	}

	public String getIteratorName() {
		return iteratorName;
	}

	public String getIteratorClass() {
		return iteratorClass;
	}

	public Map<String, String> getOptions(
			final Map<String, String> existingOptions ) {
		return optionProvider.getOptions(existingOptions);
	}

	public static interface OptionProvider
	{
		public Map<String, String> getOptions(
				Map<String, String> existingOptions );
	}
}
