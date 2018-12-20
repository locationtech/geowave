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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.persist.Persistable;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

public class InsertionIds implements
		Persistable
{
	private Collection<SinglePartitionInsertionIds> partitionKeys;
	private List<ByteArray> compositeInsertionIds;
	private int size = -1;

	public InsertionIds() {
		partitionKeys = new ArrayList<SinglePartitionInsertionIds>();
	}

	public InsertionIds(
			final List<ByteArray> sortKeys ) {
		this(
				new SinglePartitionInsertionIds(
						null,
						sortKeys));
	}

	public InsertionIds(
			final ByteArray partitionKey ) {
		this(
				new SinglePartitionInsertionIds(
						partitionKey));
	}

	public InsertionIds(
			final ByteArray partitionKey,
			final List<ByteArray> sortKeys ) {
		this(
				new SinglePartitionInsertionIds(
						partitionKey,
						sortKeys));
	}

	public InsertionIds(
			final SinglePartitionInsertionIds singePartitionKey ) {
		this(
				Arrays.asList(singePartitionKey));
	}

	public InsertionIds(
			final Collection<SinglePartitionInsertionIds> partitionKeys ) {
		this.partitionKeys = partitionKeys;
	}

	public Collection<SinglePartitionInsertionIds> getPartitionKeys() {
		return partitionKeys;
	}

	public boolean isEmpty() {
		if (compositeInsertionIds != null) {
			return compositeInsertionIds.isEmpty();
		}
		if ((partitionKeys == null) || partitionKeys.isEmpty()) {
			return true;
		}
		return false;
	}

	public boolean hasDuplicates() {
		if (compositeInsertionIds != null) {
			return compositeInsertionIds.size() >= 2;
		}
		if ((partitionKeys == null) || partitionKeys.isEmpty()) {
			return false;
		}
		if (partitionKeys.size() > 1) {
			return true;
		}
		final SinglePartitionInsertionIds partition = partitionKeys.iterator().next();
		if ((partition.getSortKeys() == null) || (partition.getSortKeys().size() <= 1)) {
			return false;
		}
		return true;
	}

	public int getSize() {
		if (size >= 0) {
			return size;
		}
		if (compositeInsertionIds != null) {
			size = compositeInsertionIds.size();
			return size;
		}
		if ((partitionKeys == null) || partitionKeys.isEmpty()) {
			size = 0;
			return size;
		}
		int internalSize = 0;
		for (final SinglePartitionInsertionIds k : partitionKeys) {
			final List<ByteArray> i = k.getCompositeInsertionIds();
			if ((i != null) && !i.isEmpty()) {
				internalSize += i.size();
			}
		}
		size = internalSize;
		return size;
	}

	public QueryRanges asQueryRanges() {
		return new QueryRanges(
				Collections2.transform(
						partitionKeys,
						new Function<SinglePartitionInsertionIds, SinglePartitionQueryRanges>() {
							@Override
							public SinglePartitionQueryRanges apply(
									final SinglePartitionInsertionIds input ) {
								return new SinglePartitionQueryRanges(
										input.getPartitionKey(),
										Collections2.transform(
												input.getSortKeys(),
												new Function<ByteArray, ByteArrayRange>() {
													@Override
													public ByteArrayRange apply(
															final ByteArray input ) {
														return new ByteArrayRange(
																input,
																input,
																false);
													}
												}));
							}
						}));
	}

	public List<ByteArray> getCompositeInsertionIds() {
		if (compositeInsertionIds != null) {
			return compositeInsertionIds;
		}
		if ((partitionKeys == null) || partitionKeys.isEmpty()) {
			return Collections.EMPTY_LIST;
		}
		final List<ByteArray> internalCompositeInsertionIds = new ArrayList<>();
		for (final SinglePartitionInsertionIds k : partitionKeys) {
			final List<ByteArray> i = k.getCompositeInsertionIds();
			if ((i != null) && !i.isEmpty()) {
				internalCompositeInsertionIds.addAll(i);
			}
		}
		compositeInsertionIds = internalCompositeInsertionIds;
		return compositeInsertionIds;
	}

	public boolean contains(
			final ByteArray partitionKey,
			final ByteArray sortKey ) {
		for (final SinglePartitionInsertionIds p : partitionKeys) {
			if (((partitionKey == null) && (p.getPartitionKey() == null))
					|| ((partitionKey != null) && partitionKey.equals(p.getPartitionKey()))) {
				// partition key matches find out if sort key is contained
				if (sortKey == null) {
					return true;
				}
				if ((p.getSortKeys() != null) && p.getSortKeys().contains(
						sortKey)) {
					return true;
				}
				return false;
			}
		}
		return false;
	}

	public Pair<ByteArray, ByteArray> getFirstPartitionAndSortKeyPair() {
		if (partitionKeys == null) {
			return null;
		}
		for (final SinglePartitionInsertionIds p : partitionKeys) {
			if ((p.getSortKeys() != null) && !p.getSortKeys().isEmpty()) {
				return new ImmutablePair<ByteArray, ByteArray>(
						p.getPartitionKey(),
						p.getSortKeys().get(
								0));
			}
			else if ((p.getPartitionKey() != null)) {
				return new ImmutablePair<ByteArray, ByteArray>(
						p.getPartitionKey(),
						null);
			}
		}
		return null;
	}

	@Override
	public byte[] toBinary() {
		if ((partitionKeys != null) && !partitionKeys.isEmpty()) {
			final List<byte[]> partitionKeysBinary = new ArrayList<>(
					partitionKeys.size());
			int totalSize = VarintUtils.unsignedIntByteLength(partitionKeys.size());
			for (final SinglePartitionInsertionIds id : partitionKeys) {
				final byte[] binary = id.toBinary();
				totalSize += (VarintUtils.unsignedIntByteLength(binary.length) + binary.length);
				partitionKeysBinary.add(binary);
			}
			final ByteBuffer buf = ByteBuffer.allocate(totalSize);
			VarintUtils.writeUnsignedInt(
					partitionKeys.size(),
					buf);
			for (final byte[] binary : partitionKeysBinary) {
				VarintUtils.writeUnsignedInt(
						binary.length,
						buf);
				buf.put(binary);
			}
			return buf.array();
		}
		else {
			return ByteBuffer.allocate(
					4).putInt(
					0).array();
		}
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int size = VarintUtils.readUnsignedInt(buf);
		if (size > 0) {
			partitionKeys = new ArrayList<>(
					size);
			for (int i = 0; i < size; i++) {
				final int length = VarintUtils.readUnsignedInt(buf);
				final byte[] pBytes = new byte[length];
				buf.get(pBytes);
				final SinglePartitionInsertionIds pId = new SinglePartitionInsertionIds();
				pId.fromBinary(pBytes);
				partitionKeys.add(pId);
			}
		}
		else {
			partitionKeys = new ArrayList<>();
		}
	}
}
