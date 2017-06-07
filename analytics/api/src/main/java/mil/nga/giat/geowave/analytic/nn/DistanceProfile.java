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
package mil.nga.giat.geowave.analytic.nn;

/**
 * Retain distance information.
 * 
 */
public class DistanceProfile<CONTEXT_TYPE>
{
	private double distance;
	private CONTEXT_TYPE context;

	public DistanceProfile() {

	}

	public DistanceProfile(
			double distance,
			CONTEXT_TYPE context ) {
		super();
		this.distance = distance;
		this.context = context;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(
			double distance ) {
		this.distance = distance;
	}

	/**
	 * 
	 * distance function specific information
	 */
	public CONTEXT_TYPE getContext() {
		return context;
	}

	public void setContext(
			CONTEXT_TYPE context ) {
		this.context = context;
	}

	@Override
	public String toString() {
		return "DistanceProfile [distance=" + distance + ", context=" + context + "]";
	}

}
