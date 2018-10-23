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

import java.util.Arrays;

import com.google.common.primitives.UnsignedBytes;

public class GeoWaveMetadata implements
		Comparable<GeoWaveMetadata>
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((primaryId == null || primaryId.length == 0) ? 0 : Arrays.hashCode(primaryId));
		result = (prime * result)
				+ ((secondaryId == null || secondaryId.length == 0) ? 0 : Arrays.hashCode(secondaryId));
		result = (prime * result) + ((value == null || value.length == 0) ? 0 : Arrays.hashCode(value));
		result = (prime * result) + ((visibility == null || visibility.length == 0) ? 0 : Arrays.hashCode(visibility));
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final GeoWaveMetadata other = (GeoWaveMetadata) obj;
		byte[] otherComp = other.primaryId != null && other.primaryId.length == 0 ? null : other.primaryId;
		byte[] thisComp = primaryId != null && primaryId.length == 0 ? null : primaryId;
		if (!Arrays.equals(
				thisComp,
				otherComp)) {
			return false;
		}
		otherComp = other.secondaryId != null && other.secondaryId.length == 0 ? null : other.secondaryId;
		thisComp = secondaryId != null && secondaryId.length == 0 ? null : secondaryId;
		if (!Arrays.equals(
				otherComp,
				thisComp)) {
			return false;
		}
		otherComp = other.value != null && other.value.length == 0 ? null : other.value;
		thisComp = value != null && value.length == 0 ? null : value;
		if (!Arrays.equals(
				otherComp,
				thisComp)) {
			return false;
		}
		otherComp = other.visibility != null && other.visibility.length == 0 ? null : other.visibility;
		thisComp = visibility != null && visibility.length == 0 ? null : visibility;
		if (!Arrays.equals(
				otherComp,
				thisComp)) {
			return false;
		}
		return true;
	}

	@Override
	public int compareTo(
			GeoWaveMetadata obj ) {
		if (this == obj) {
			return 0;
		}
		if (obj == null) {
			return 1;
		}
		if (getClass() != obj.getClass()) {
			return 1;
		}
		final GeoWaveMetadata other = (GeoWaveMetadata) obj;
		byte[] otherComp = other.primaryId == null ? new byte[0] : other.primaryId;
		byte[] thisComp = primaryId == null ? new byte[0] : primaryId;
		if (UnsignedBytes.lexicographicalComparator().compare(
				thisComp,
				otherComp) != 0) {
			return UnsignedBytes.lexicographicalComparator().compare(
					thisComp,
					otherComp);
		}
		otherComp = other.secondaryId == null ? new byte[0] : other.secondaryId;
		thisComp = secondaryId == null ? new byte[0] : secondaryId;
		if (UnsignedBytes.lexicographicalComparator().compare(
				thisComp,
				otherComp) != 0) {
			return UnsignedBytes.lexicographicalComparator().compare(
					thisComp,
					otherComp);
		}
		otherComp = other.value == null ? new byte[0] : other.value;
		thisComp = value == null ? new byte[0] : value;
		if (UnsignedBytes.lexicographicalComparator().compare(
				thisComp,
				otherComp) != 0) {
			return UnsignedBytes.lexicographicalComparator().compare(
					thisComp,
					otherComp);
		}
		otherComp = other.visibility == null ? new byte[0] : other.visibility;
		thisComp = visibility == null ? new byte[0] : visibility;
		if (UnsignedBytes.lexicographicalComparator().compare(
				thisComp,
				otherComp) != 0) {
			return UnsignedBytes.lexicographicalComparator().compare(
					thisComp,
					otherComp);
		}
		return 0;
	}
}
