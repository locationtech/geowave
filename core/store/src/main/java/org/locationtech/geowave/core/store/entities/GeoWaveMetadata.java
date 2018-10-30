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

public class GeoWaveMetadata
{
	private final byte[] primaryId;
	private final byte[] secondaryId;
	private final byte[] visibility;
	private final byte[] value;

	public GeoWaveMetadata(
			final byte[] primaryId,
			final byte[] secondaryId,
			final byte[] visibility,
			final byte[] value ) {
		this.primaryId = primaryId;
		this.secondaryId = secondaryId;
		this.visibility = visibility;
		this.value = value;
	}

	public byte[] getPrimaryId() {
		return primaryId;
	}

	public byte[] getSecondaryId() {
		return secondaryId;
	}

	public byte[] getVisibility() {
		return visibility;
	}

	public byte[] getValue() {
		return value;
	}
}
