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
package org.locationtech.geowave.core.store.entities;

public class GeoWaveRowImpl implements
		GeoWaveRow
{
	private final GeoWaveKey key;
	private final GeoWaveValue[] fieldValues;

	public GeoWaveRowImpl(
			final GeoWaveKey key,
			final GeoWaveValue[] fieldValues ) {
		this.key = key;
		this.fieldValues = fieldValues;
	}

	@Override
	public byte[] getDataId() {
		return key.getDataId();
	}

	@Override
	public short getAdapterId() {
		return key.getAdapterId();
	}

	@Override
	public byte[] getSortKey() {
		return key.getSortKey();
	}

	@Override
	public byte[] getPartitionKey() {
		return key.getPartitionKey();
	}

	@Override
	public int getNumberOfDuplicates() {
		return key.getNumberOfDuplicates();
	}

	public GeoWaveKey getKey() {
		return key;
	}

	@Override
	public GeoWaveValue[] getFieldValues() {
		return fieldValues;
	}
}
