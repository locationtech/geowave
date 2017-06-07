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
package mil.nga.giat.geowave.analytic.clustering;

import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.apache.commons.codec.binary.Hex;

public class NeighborData<T> implements
		Comparable<NeighborData<T>>
{
	private T element;
	private ByteArrayId id;
	private double distance;

	public NeighborData() {}

	public NeighborData(
			final T element,
			final ByteArrayId id,
			final double distance ) {
		super();
		this.element = element;
		this.id = id;
		this.distance = distance;
	}

	public NeighborData(
			final NeighborData<T> element,
			final double distance ) {
		super();
		this.element = element.getElement();
		this.id = element.getId();
		this.distance = distance;
	}

	public ByteArrayId getId() {
		return id;
	}

	protected void setId(
			final ByteArrayId id ) {
		this.id = id;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(
			final double distance ) {
		this.distance = distance;
	}

	public T getElement() {
		return element;
	}

	protected void setElement(
			final T neighbor ) {
		this.element = neighbor;
	}

	@Override
	public int hashCode() {
		return ((element == null) ? 0 : element.hashCode());
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
		@SuppressWarnings("unchecked")
		final NeighborData<T> other = (NeighborData<T>) obj;
		if (element == null) {
			if (other.element != null) {
				return false;
			}
		}
		else if (!element.equals(other.element)) {
			return false;
		}
		return true;
	}

	@Override
	public int compareTo(
			final NeighborData<T> otherNNData ) {
		final int dist = Double.compare(
				distance,
				otherNNData.distance);
		// do not care about the ordering based on the neighbor data.
		// just need to force some ordering if they are not the same.
		return dist == 0 ? hashCode() - otherNNData.hashCode() : dist;
	}

	@Override
	public String toString() {
		return (id == null ? "" : Hex.encodeHexString(id.getBytes()) + ":") + element.toString() + "(" + this.distance
				+ ")";
	}
}
