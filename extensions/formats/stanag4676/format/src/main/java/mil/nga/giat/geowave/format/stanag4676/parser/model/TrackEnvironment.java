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

//STANAG 4676
/**
 * Enumeration Provides an estimate of the type of environment in which a track
 * is computed.
 */
public enum TrackEnvironment {
	/**
	 * On a water body (ie: water borne vessels)
	 */
	SURFACE(
			"SURFACE"),

	/**
	 * Under the surface of a water body.
	 */
	SUBSURFACE(
			"SUBSURFACE"),

	/**
	 * On the surface of dry land.
	 */
	LAND(
			"LAND"),

	/**
	 * Between sea level and the Karman line, which is the altitude of 100
	 * kilometres (62 mi).
	 */
	AIR(
			"AIR"),

	/**
	 * Above the Karman line, which is the altitude of 100 kilometres (62 mi).
	 */
	SPACE(
			"SPACE"),

	/**
	 * The environment is not known.
	 */
	UNKNOWN(
			"UNKNOWN");

	private String value;

	TrackEnvironment() {
		this.value = TrackEnvironment.values()[0].toString();
	}

	TrackEnvironment(
			final String value ) {
		this.value = value;
	}

	public static TrackEnvironment fromString(
			String value ) {
		for (final TrackEnvironment item : TrackEnvironment.values()) {
			if (item.toString().equals(
					value)) {
				return item;
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return value;
	}
}
