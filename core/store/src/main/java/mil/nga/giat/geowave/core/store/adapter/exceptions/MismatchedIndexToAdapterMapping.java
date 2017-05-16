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
package mil.nga.giat.geowave.core.store.adapter.exceptions;

import java.io.IOException;
import java.util.Arrays;

import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;

/*
 * Thrown if a mapping exists and the new mapping is not equivalent.
 */
public class MismatchedIndexToAdapterMapping extends
		IOException
{

	private static final long serialVersionUID = 1L;

	public MismatchedIndexToAdapterMapping(
			final AdapterToIndexMapping adapterMapping ) {
		super(
				"Adapter " + adapterMapping.getAdapterId() + " already associated to indices " + Arrays.asList(
						adapterMapping.getIndexIds()).toString());
	}

}
