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
package mil.nga.giat.geowave.analytic.mapreduce.nn;

public class NNData<T> implements
		Comparable<NNData<T>>
{
	private T neighbor;
	private double distance;

	public NNData() {}

	public NNData(
			final T neighbor,
			final double distance ) {
		super();
		this.neighbor = neighbor;
		this.distance = distance;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(
			final double distance ) {
		this.distance = distance;
	}

	protected T getNeighbor() {
		return neighbor;
	}

	protected void setNeighbor(
			final T neighbor ) {
		this.neighbor = neighbor;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(distance);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((neighbor == null) ? 0 : neighbor.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		@SuppressWarnings("unchecked")
		NNData<T> other = (NNData<T>) obj;
		if (Double.doubleToLongBits(distance) != Double.doubleToLongBits(other.distance)) return false;
		if (neighbor == null) {
			if (other.neighbor != null) return false;
		}
		else if (!neighbor.equals(other.neighbor)) return false;
		return true;
	}

	@Override
	public int compareTo(
			NNData<T> otherNNData ) {
		final int dist = Double.compare(
				distance,
				otherNNData.distance);
		// do not care about the ordering based on the neighbor data.
		// just need to force some ordering if they are not the same.
		return dist == 0 ? hashCode() - otherNNData.hashCode() : dist;
	}

}
