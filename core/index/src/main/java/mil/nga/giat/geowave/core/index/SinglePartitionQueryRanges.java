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
package mil.nga.giat.geowave.core.index;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SinglePartitionQueryRanges
{
	private final ByteArrayId partitionKey;

	private final Collection<ByteArrayRange> sortKeyRanges;

	public SinglePartitionQueryRanges(
			final ByteArrayId partitionKey,
			final Collection<ByteArrayRange> sortKeyRanges ) {
		this.partitionKey = partitionKey;
		this.sortKeyRanges = sortKeyRanges;
	}

	public SinglePartitionQueryRanges(
			final ByteArrayId partitionKey ) {
		this.partitionKey = partitionKey;
		sortKeyRanges = null;
	}

	public SinglePartitionQueryRanges(
			final List<ByteArrayRange> sortKeyRanges ) {
		this.sortKeyRanges = sortKeyRanges;
		partitionKey = null;
	}

	public SinglePartitionQueryRanges(
			final ByteArrayRange singleSortKeyRange ) {
		sortKeyRanges = Collections.singletonList(singleSortKeyRange);
		partitionKey = null;
	}

	public ByteArrayId getPartitionKey() {
		return partitionKey;
	}

	public Collection<ByteArrayRange> getSortKeyRanges() {
		return sortKeyRanges;
	}

	public ByteArrayRange getSingleRange() {
		ByteArrayId start = null;
		ByteArrayId end = null;

		for (final ByteArrayRange range : sortKeyRanges) {
			if ((start == null) || (range.getStart().compareTo(
					start) < 0)) {
				start = range.getStart();
			}
			if ((end == null) || (range.getEnd().compareTo(
					end) > 0)) {
				end = range.getEnd();
			}
		}
		return new ByteArrayRange(
				start,
				end);
	}
}
