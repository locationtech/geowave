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
package org.locationtech.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.locationtech.geowave.core.index.persist.Persistable;

public class SinglePartitionInsertionIds implements
		Persistable
{
	private List<ByteArray> compositeInsertionIds;
	private ByteArray partitionKey;
	private List<ByteArray> sortKeys;

	public SinglePartitionInsertionIds() {}

	public SinglePartitionInsertionIds(
			final ByteArray partitionKey ) {
		this(
				partitionKey,
				(ByteArray) null);
	}

	public SinglePartitionInsertionIds(
			final ByteArray partitionKey,
			final ByteArray sortKey ) {
		this.partitionKey = partitionKey;
		sortKeys = sortKey == null ? null : Arrays.asList(sortKey);
	}

	public SinglePartitionInsertionIds(
			final ByteArray partitionKey,
			final SinglePartitionInsertionIds insertionId2 ) {
		this(
				new SinglePartitionInsertionIds(
						partitionKey,
						(List<ByteArray>) null),
				insertionId2);
	}

	public SinglePartitionInsertionIds(
			final SinglePartitionInsertionIds insertionId1,
			final SinglePartitionInsertionIds insertionId2 ) {
		partitionKey = new ByteArray(
				ByteArrayUtils.combineArrays(
						insertionId1.partitionKey.getBytes(),
						insertionId2.partitionKey.getBytes()));
		if ((insertionId1.sortKeys == null) || insertionId1.sortKeys.isEmpty()) {
			sortKeys = insertionId2.sortKeys;
		}
		else if ((insertionId2.sortKeys == null) || insertionId2.sortKeys.isEmpty()) {
			sortKeys = insertionId1.sortKeys;
		}
		else {
			// use all permutations of range keys
			sortKeys = new ArrayList<ByteArray>(
					insertionId1.sortKeys.size() * insertionId2.sortKeys.size());
			for (final ByteArray sortKey1 : insertionId1.sortKeys) {
				for (final ByteArray sortKey2 : insertionId2.sortKeys) {
					sortKeys.add(new ByteArray(
							ByteArrayUtils.combineArrays(
									sortKey1.getBytes(),
									sortKey2.getBytes())));
				}
			}
		}
	}

	public SinglePartitionInsertionIds(
			final ByteArray partitionKey,
			final List<ByteArray> sortKeys ) {
		this.partitionKey = partitionKey;
		this.sortKeys = sortKeys;
	}

	public List<ByteArray> getCompositeInsertionIds() {
		if (compositeInsertionIds != null) {
			return compositeInsertionIds;
		}

		if ((sortKeys == null) || sortKeys.isEmpty()) {
			compositeInsertionIds = Arrays.asList(partitionKey);
			return compositeInsertionIds;
		}

		if (partitionKey == null) {
			compositeInsertionIds = sortKeys;
			return compositeInsertionIds;
		}

		final List<ByteArray> internalInsertionIds = new ArrayList<>(
				sortKeys.size());
		for (final ByteArray sortKey : sortKeys) {
			internalInsertionIds.add(new ByteArray(
					ByteArrayUtils.combineArrays(
							partitionKey.getBytes(),
							sortKey.getBytes())));
		}
		compositeInsertionIds = internalInsertionIds;
		return compositeInsertionIds;
	}

	public ByteArray getPartitionKey() {
		return partitionKey;
	}

	public List<ByteArray> getSortKeys() {
		return sortKeys;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((partitionKey == null) ? 0 : partitionKey.hashCode());
		result = (prime * result) + ((sortKeys == null) ? 0 : sortKeys.hashCode());
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
		final SinglePartitionInsertionIds other = (SinglePartitionInsertionIds) obj;
		if (partitionKey == null) {
			if (other.partitionKey != null) {
				return false;
			}
		}
		else if (!partitionKey.equals(other.partitionKey)) {
			return false;
		}
		if (sortKeys == null) {
			if (other.sortKeys != null) {
				return false;
			}
		}
		else if (!sortKeys.equals(other.sortKeys)) {
			return false;
		}
		return true;
	}

	@Override
	public byte[] toBinary() {
		int pLength;
		if (partitionKey == null) {
			pLength = 0;
		}
		else {
			pLength = partitionKey.getBytes().length;
		}
		int sSize;
		int byteBufferSize = 8 + pLength;
		if (sortKeys == null) {
			sSize = 0;
		}
		else {
			sSize = sortKeys.size();
			byteBufferSize += (4 * sSize);
			for (final ByteArray sKey : sortKeys) {
				byteBufferSize += sKey.getBytes().length;
			}
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteBufferSize);
		buf.putInt(pLength);
		if (pLength > 0) {
			buf.put(partitionKey.getBytes());
		}
		buf.putInt(sSize);

		if (sSize > 0) {
			for (final ByteArray sKey : sortKeys) {
				buf.putInt(sKey.getBytes().length);
				buf.put(sKey.getBytes());
			}
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int pLength = buf.getInt();
		if (pLength > 0) {
			final byte[] pBytes = new byte[pLength];
			buf.get(pBytes);
			partitionKey = new ByteArray(
					pBytes);
		}
		else {
			partitionKey = null;
		}
		final int sSize = buf.getInt();
		if (sSize > 0) {
			sortKeys = new ArrayList<>(
					sSize);
			for (int i = 0; i < sSize; i++) {
				final int keyLength = buf.getInt();
				final byte[] sortKey = new byte[keyLength];
				buf.get(sortKey);
				sortKeys.add(new ByteArray(
						sortKey));
			}
		}
		else {
			sortKeys = null;
		}
	}

}
