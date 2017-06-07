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
package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/***
 * Defines a unit interval on a number line
 * 
 */
public class ByteArrayRange implements
		Comparable<ByteArrayRange>
{
	protected ByteArrayId start;
	protected ByteArrayId end;
	protected boolean singleValue;

	/***
	 * 
	 * @param start
	 *            start of unit interval
	 * @param end
	 *            end of unit interval
	 */
	public ByteArrayRange(
			final ByteArrayId start,
			final ByteArrayId end ) {
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
			final ByteArrayId start,
			final ByteArrayId end,
			boolean singleValue ) {
		this.start = start;
		this.end = end;
		this.singleValue = singleValue;
	}

	public ByteArrayId getStart() {
		return start;
	}

	public ByteArrayId getEnd() {
		return end;
	}

	public boolean isSingleValue() {
		return singleValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((end == null) ? 0 : end.hashCode());
		result = prime * result + (singleValue ? 1231 : 1237);
		result = prime * result + ((start == null) ? 0 : start.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		ByteArrayRange other = (ByteArrayRange) obj;
		if (end == null) {
			if (other.end != null) return false;
		}
		else if (!end.equals(other.end)) return false;
		if (singleValue != other.singleValue) return false;
		if (start == null) {
			if (other.start != null) return false;
		}
		else if (!start.equals(other.start)) return false;
		return true;
	}

	public boolean intersects(
			ByteArrayRange other ) {
		return ((getStart().compareTo(other.getEnd())) <= 0 && (getEnd().compareTo(other.getStart())) >= 0);
	}

	public ByteArrayRange intersection(
			ByteArrayRange other ) {
		return new ByteArrayRange(
				this.start.compareTo(other.start) <= 0 ? other.start : this.start,
				this.end.compareTo(other.end) >= 0 ? other.end : this.end);
	}

	public ByteArrayRange union(
			ByteArrayRange other ) {
		return new ByteArrayRange(
				this.start.compareTo(other.start) <= 0 ? this.start : other.start,
				this.end.compareTo(other.end) >= 0 ? this.end : other.end);
	}

	@Override
	public int compareTo(
			ByteArrayRange other ) {
		final int diff = getStart().compareTo(
				other.getStart());
		return diff != 0 ? diff : getEnd().compareTo(
				other.getEnd());
	}

	public static enum MergeOperation {
		UNION,
		INTERSECTION
	}

	public static final List<ByteArrayRange> mergeIntersections(
			List<ByteArrayRange> ranges,
			MergeOperation op ) {
		// sort order so the first range can consume following ranges
		Collections.<ByteArrayRange> sort(ranges);
		final List<ByteArrayRange> result = new ArrayList<ByteArrayRange>();
		for (int i = 0; i < ranges.size();) {
			ByteArrayRange r1 = ranges.get(i);
			int j = i + 1;
			for (; j < ranges.size(); j++) {
				final ByteArrayRange r2 = ranges.get(j);
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
