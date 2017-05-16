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
package mil.nga.giat.geowave.core.geotime.store.query;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class TemporalConstraints
{
	private LinkedList<TemporalRange> constraints = new LinkedList<TemporalRange>();
	private String name;

	public static final TemporalRange FULL_RANGE = new TemporalRange(
			TemporalRange.START_TIME,
			TemporalRange.END_TIME);

	public TemporalConstraints() {

	}

	public String getName() {
		return name;
	}

	public void empty() {
		constraints.clear();
	}

	public TemporalConstraints(
			String name ) {
		this.name = name;
	}

	public TemporalConstraints(
			List<TemporalRange> ranges,
			String name ) {
		this.constraints.addAll(ranges);
		this.name = name;
	}

	public TemporalConstraints(
			TemporalRange range,
			String name ) {
		this.constraints.add(range);
		this.name = name;
	}

	public void replaceWithIntersections(
			final TemporalConstraints constraints ) {
		this.constraints = TemporalConstraints.findIntersections(
				this,
				constraints).constraints;
	}

	public void replaceWithMerged(
			final TemporalConstraints constraints ) {
		this.constraints = TemporalConstraints.merge(
				this,
				constraints).constraints;
	}

	public void add(
			final TemporalRange range ) {
		int pos = 0;
		TemporalRange nextNeighbor = null;
		for (final TemporalRange aRange : constraints) {
			nextNeighbor = aRange;
			if (nextNeighbor.getStartTime().after(
					range.getStartTime())) {
				break;
			}
			else if (nextNeighbor.getEndTime().after(
					range.getStartTime()) || nextNeighbor.getEndTime().equals(
					range.getStartTime())) {
				if (range.getEndTime().before(
						nextNeighbor.getEndTime())) {
					// subsummed
					return;
				}
				else {
					// replaced with larger range
					constraints.set(
							pos,
							new TemporalRange(
									nextNeighbor.getStartTime(),
									range.getEndTime()));
					return;
				}
			}
			pos++;
		}
		if ((nextNeighbor != null) && nextNeighbor.getStartTime().before(
				range.getEndTime())) {
			constraints.add(
					pos,
					new TemporalRange(
							range.getStartTime(),
							TemporalConstraints.max(
									nextNeighbor.getEndTime(),
									range.getEndTime())));
		}
		else {
			constraints.add(
					pos,
					range);
		}
	}

	public static final Date max(
			final Date one,
			final Date two ) {
		return one.before(two) ? two : one;
	}

	public static final Date min(
			final Date one,
			final Date two ) {
		return one.before(two) ? one : two;
	}

	public Date getMinOr(
			final Date min,
			int exclusivityIncrement ) {
		return (constraints.isEmpty()) ? min : exclusivityIncrement == 0 ? constraints.getFirst().getStartTime()
				: new Date(
						constraints.getFirst().getStartTime().getTime() + exclusivityIncrement);
	}

	public Date getMaxOr(
			final Date max,
			int exclusivityIncrement ) {
		return (constraints.isEmpty()) ? max : exclusivityIncrement == 0 ? constraints.getLast().getEndTime()
				: new Date(
						constraints.getLast().getEndTime().getTime() + exclusivityIncrement);
	}

	public boolean isEmpty() {
		return constraints.isEmpty();
	}

	public TemporalRange getEndRange() {
		return (constraints.isEmpty()) ? FULL_RANGE : constraints.getLast();
	}

	public TemporalRange getStartRange() {
		return (constraints.isEmpty()) ? FULL_RANGE : constraints.getFirst();
	}

	public List<TemporalRange> getRanges() {
		return constraints == null ? Collections.<TemporalRange> emptyList() : constraints;
	}

	public static final TemporalConstraints findIntersections(
			final TemporalConstraints sideL,
			final TemporalConstraints sideR ) {

		if (sideL.constraints.isEmpty()) {
			return sideR;
		}
		if (sideR.constraints.isEmpty()) {
			return sideL;
		}

		final TemporalConstraints newSet = new TemporalConstraints(
				sideL.name);

		for (final TemporalRange lRange : sideL.constraints) {
			for (final TemporalRange rRange : sideR.constraints) {
				if (lRange.getEndTime().before(
						rRange.getStartTime()) || rRange.getEndTime().before(
						lRange.getStartTime())) {
					continue;
				}
				newSet.add(new TemporalRange(
						max(
								lRange.getStartTime(),
								rRange.getStartTime()),
						min(
								lRange.getEndTime(),
								rRange.getEndTime())));
			}
		}
		return newSet;
	}

	public static final TemporalConstraints merge(
			final TemporalConstraints left,
			final TemporalConstraints right ) {
		if (left.isEmpty()) {
			return right;
		}
		if (right.isEmpty()) {
			return left;
		}

		final TemporalConstraints newSetOfRanges = new TemporalConstraints(
				left.name);
		newSetOfRanges.constraints.addAll(left.constraints);
		for (final TemporalRange range : right.constraints) {
			newSetOfRanges.add(range);
		}
		return newSetOfRanges;
	}

	public byte[] toBinary() {
		final ByteBuffer buffer = ByteBuffer.allocate(4 + (constraints.size() * TemporalRange.getBufferSize()));
		buffer.putInt(constraints.size());

		for (final TemporalRange range : constraints) {
			buffer.put(range.toBinary());
		}

		return buffer.array();
	}

	public void fromBinary(
			final byte[] data ) {
		final ByteBuffer buffer = ByteBuffer.wrap(data);

		final int s = buffer.getInt();
		final byte[] rangeBuf = new byte[TemporalRange.getBufferSize()];
		for (int i = 0; i < s; i++) {
			buffer.get(rangeBuf);
			final TemporalRange range = new TemporalRange();
			range.fromBinary(rangeBuf);
			add(range);
		}

	}

	@Override
	public String toString() {
		return "TemporalConstraints [constraints=" + constraints + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((constraints == null) ? 0 : constraints.hashCode());
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
		final TemporalConstraints other = (TemporalConstraints) obj;
		if (constraints == null) {
			if (other.constraints != null) {
				return false;
			}
		}
		else if (!constraints.equals(other.constraints)) {
			return false;
		}
		return true;
	}

}
