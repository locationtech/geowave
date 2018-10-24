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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/***
 * Defines a unit interval on a number line
 *
 */
public class ByteArrayRange implements
		Comparable<ByteArrayRange>
{
	protected ByteArray start;
	protected ByteArray end;
	protected boolean singleValue;

	/***
	 *
	 * @param start
	 *            start of unit interval
	 * @param end
	 *            end of unit interval
	 */
	public ByteArrayRange(
			final ByteArray start,
			final ByteArray end ) {
		this(
				start,
				end,
				false);
	}

	/***
	 *
	 * @param start
	 *            start of unit interval
	 * @param end
	 *            end of unit interval
	 */
	public ByteArrayRange(
			final ByteArray start,
			final ByteArray end,
			final boolean singleValue ) {
		this.start = start;
		this.end = end;
		this.singleValue = singleValue;
	}

	public ByteArray getStart() {
		return start;
	}

	public ByteArray getEnd() {
		return end;
	}

	public ByteArray getEndAsNextPrefix() {
		return new ByteArray(
				end.getNextPrefix());
	}

	public boolean isSingleValue() {
		return singleValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((end == null) ? 0 : end.hashCode());
		result = (prime * result) + (singleValue ? 1231 : 1237);
		result = (prime * result) + ((start == null) ? 0 : start.hashCode());
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
		final ByteArrayRange other = (ByteArrayRange) obj;
		if (end == null) {
			if (other.end != null) {
				return false;
			}
		}
		else if (!end.equals(other.end)) {
			return false;
		}
		if (singleValue != other.singleValue) {
			return false;
		}
		if (start == null) {
			if (other.start != null) {
				return false;
			}
		}
		else if (!start.equals(other.start)) {
			return false;
		}
		return true;
	}

	public boolean intersects(
			final ByteArrayRange other ) {
		if (isSingleValue()) {
			if (other.isSingleValue()) {
				return getStart().equals(
						other.getStart());
			}
			return false;
		}
		return (((getStart().compareTo(other.getEndAsNextPrefix())) < 0) && ((getEndAsNextPrefix().compareTo(other
				.getStart())) > 0));
	}

	public ByteArrayRange intersection(
			final ByteArrayRange other ) {
		return new ByteArrayRange(
				start.compareTo(other.start) <= 0 ? other.start : start,
				getEndAsNextPrefix().compareTo(
						other.getEndAsNextPrefix()) >= 0 ? other.end : end);
	}

	public ByteArrayRange union(
			final ByteArrayRange other ) {
		return new ByteArrayRange(
				start.compareTo(other.start) <= 0 ? start : other.start,
				getEndAsNextPrefix().compareTo(
						other.getEndAsNextPrefix()) >= 0 ? end : other.end);
	}

	@Override
	public int compareTo(
			final ByteArrayRange other ) {
		final int diff = getStart().compareTo(
				other.getStart());
		return diff != 0 ? diff : getEndAsNextPrefix().compareTo(
				other.getEndAsNextPrefix());
	}

	public static enum MergeOperation {
		UNION,
		INTERSECTION
	}

	public static final Collection<ByteArrayRange> mergeIntersections(
			final Collection<ByteArrayRange> ranges,
			final MergeOperation op ) {
		List<ByteArrayRange> rangeList = new ArrayList<>(
				ranges);
		// sort order so the first range can consume following ranges
		Collections.<ByteArrayRange> sort(rangeList);
		final List<ByteArrayRange> result = new ArrayList<ByteArrayRange>();
		for (int i = 0; i < rangeList.size();) {
			ByteArrayRange r1 = rangeList.get(i);
			int j = i + 1;
			for (; j < rangeList.size(); j++) {
				final ByteArrayRange r2 = rangeList.get(j);
				if (r1.intersects(r2)) {
					if (op.equals(MergeOperation.UNION)) {
						r1 = r1.union(r2);
					}
					else {
						r1 = r1.intersection(r2);
					}
				}
				else {
					break;
				}
			}
			i = j;
			result.add(r1);
		}
		return result;
	}
}
