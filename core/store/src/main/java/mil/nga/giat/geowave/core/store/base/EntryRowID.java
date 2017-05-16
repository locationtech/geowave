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
package mil.nga.giat.geowave.core.store.base;

import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class encapsulates the elements that compose the row ID
 * 
 * 
 */
@SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "private class only accessed internally")
public class EntryRowID implements
		Comparable<EntryRowID>
{
	private final byte[] insertionId;
	private final byte[] dataId;
	private final byte[] adapterId;
	private final int numberOfDuplicates;

	public EntryRowID(
			final byte[] byteID ) {
		final byte[] metadata = Arrays.copyOfRange(
				byteID,
				byteID.length - 12,
				byteID.length);
		final ByteBuffer metadataBuf = ByteBuffer.wrap(metadata);
		final int adapterIdLength = metadataBuf.getInt();
		final int dataIdLength = metadataBuf.getInt();
		final int numberOfDuplicates = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				byteID,
				0,
				byteID.length - 12);
		final byte[] insertionId = new byte[byteID.length - 12 - adapterIdLength - dataIdLength];
		final byte[] adapterId = new byte[adapterIdLength];
		final byte[] dataId = new byte[dataIdLength];
		buf.get(insertionId);
		buf.get(adapterId);
		buf.get(dataId);
		this.insertionId = insertionId;
		this.dataId = dataId;
		this.adapterId = adapterId;
		this.numberOfDuplicates = numberOfDuplicates;
	}

	public EntryRowID(
			final byte[] indexId,
			final byte[] dataId,
			final byte[] adapterId,
			final int numberOfDuplicates ) {
		insertionId = indexId;
		this.dataId = dataId;
		this.adapterId = adapterId;
		this.numberOfDuplicates = numberOfDuplicates;
	}

	public byte[] getRowId() {
		final ByteBuffer buf = ByteBuffer.allocate(12 + dataId.length + adapterId.length + insertionId.length);
		buf.put(insertionId);
		buf.put(adapterId);
		buf.put(dataId);
		buf.putInt(adapterId.length);
		buf.putInt(dataId.length);
		buf.putInt(numberOfDuplicates);
		return buf.array();
	}

	public byte[] getInsertionId() {
		return insertionId;
	}

	public byte[] getDataId() {
		return dataId;
	}

	public byte[] getAdapterId() {
		return adapterId;
	}

	public int getNumberOfDuplicates() {
		return numberOfDuplicates;
	}

	public boolean isDeduplicationEnabled() {
		return numberOfDuplicates >= 0;
	}

	@Override
	public int compareTo(
			final EntryRowID other ) {
		final int indexIdCompare = compare(
				insertionId,
				other.insertionId);
		if (indexIdCompare != 0) {
			return indexIdCompare;
		}
		final int dataIdCompare = compare(
				dataId,
				other.dataId);
		if (dataIdCompare != 0) {
			return dataIdCompare;
		}
		final int adapterIdCompare = compare(
				adapterId,
				other.adapterId);
		if (adapterIdCompare != 0) {
			return adapterIdCompare;
		}
		return 0;

	}

	private static final int compare(
			final byte[] a,
			final byte[] b ) {
		int j = 0;
		for (final byte aByte : a) {
			if (b.length <= j) {
				break;
			}
			final int val = aByte - b[j];
			if (val != 0) {
				return val;
			}
			j++;
		}
		return a.length - b.length;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(adapterId);
		result = (prime * result) + Arrays.hashCode(dataId);
		result = (prime * result) + Arrays.hashCode(insertionId);
		result = (prime * result) + numberOfDuplicates;
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
		final EntryRowID other = (EntryRowID) obj;
		if (!Arrays.equals(
				adapterId,
				other.adapterId)) {
			return false;
		}
		if (!Arrays.equals(
				dataId,
				other.dataId)) {
			return false;
		}
		if (!Arrays.equals(
				insertionId,
				other.insertionId)) {
			return false;
		}
		if (numberOfDuplicates != other.numberOfDuplicates) {
			return false;
		}

		return true;
	}

}
