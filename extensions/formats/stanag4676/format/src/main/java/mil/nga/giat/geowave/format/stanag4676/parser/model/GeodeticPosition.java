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
package mil.nga.giat.geowave.format.stanag4676.parser.model;

public class GeodeticPosition extends
		Position
{

	public GeodeticPosition() {
		elevation = 0.0;
	}

	public GeodeticPosition(
			double latitudeDegrees,
			double longitudeDegrees ) {
		latitude = latitudeDegrees;
		longitude = longitudeDegrees;
		elevation = 0.0;
	}

	public GeodeticPosition(
			double latitudeDegrees,
			double longitudeDegrees,
			double elevationMeters ) {
		latitude = latitudeDegrees;
		longitude = longitudeDegrees;
		elevation = elevationMeters;
	}

	/**
	 * latitude in decimal degrees
	 */
	public Double latitude;

	/**
	 * longitude in decimal degrees
	 */
	public Double longitude;

	/**
	 * elevation in meters above ellipsoid (WGS84)
	 */
	public Double elevation;
}
