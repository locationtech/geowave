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
package org.locationtech.geowave.mapreduce;

import java.util.List;

import org.locationtech.geowave.core.store.api.DataTypeAdapter;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class MapReduceUtils
{
	public static List<String> idsFromAdapters(
			final List<DataTypeAdapter<Object>> adapters ) {
		return Lists.transform(
				adapters,
				new Function<DataTypeAdapter<Object>, String>() {
					@Override
					public String apply(
							final DataTypeAdapter<Object> adapter ) {
						return adapter == null ? "" : adapter.getTypeName();
					}
				});
	}
}
