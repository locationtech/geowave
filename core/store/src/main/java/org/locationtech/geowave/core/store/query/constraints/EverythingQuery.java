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
package org.locationtech.geowave.core.store.query.constraints;

import java.util.Collections;
import java.util.List;

import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class EverythingQuery implements
		QueryConstraints
{
	public EverythingQuery() {}

	@Override
	public List<QueryFilter> createFilters(
			final Index index ) {
		return Collections.<QueryFilter> emptyList();
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final Index index ) {
		return Collections.emptyList();
	}

	@Override
	public byte[] toBinary() {
		return new byte[0];
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public int hashCode() {
		return getClass().hashCode();
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (obj == null) {
			return false;
		}
		return getClass() == obj.getClass();
	}

}
