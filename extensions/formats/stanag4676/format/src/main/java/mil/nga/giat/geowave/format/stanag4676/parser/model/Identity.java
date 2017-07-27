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
 * Enumeration Provides the estimated identity/status of an object being
 * tracked. Values in accordance with STANAG 1241.
 */
public enum Identity {
	/**
	 * Per STANAG 1241 - an evaluated track, object or entity, which does not
	 * meet the criteria for any other standard identity.
	 */
	UNKNOWN(
			"UNKNOWN"),

	/**
	 * Per STANAG 1241 - a track, object or entity which is assumed to be friend
	 * or neutral because of its characteristics, behavior or origin.
	 */
	ASSUMED_FRIEND(
			"ASSUMED_FRIEND"),

	/**
	 * Per STANAG 1241 - an allied/coalition military track, object or entity; -
	 * a track, object or entity, supporting friendly forces and belonging to an
	 * allied/coalition nation or a declared or recognized friendly faction or
	 * group.
	 */
	FRIEND(
			"FRIEND"),

	/**
	 * Per STANAG 1241 - military or civilian track, object or entity, neither
	 * belonging to allied/coalition military forces nor to opposing military
	 * forces, whose characteristics, behavior, origin or nationality indicates
	 * that it is neither supporting nor opposing friendly forces or their
	 * mission.
	 */
	NEUTRAL(
			"NEUTRAL"),

	/**
	 * Per STANAG 1241 - a track, object or entity whose characteristics,
	 * behavior or origin indicate that it potentially belongs to opposing
	 * forces or potentially poses a threat to friendly forces or their mission.
	 */
	SUSPECT(
			"SUSPECT"),

	/**
	 * Per STANAG 1241 - a track, object or entity whose characteristics,
	 * behavior or origin indicate that it belongs to opposing forces or poses a
	 * threat to friendly forces or their mission.
	 */
	HOSTILE(
			"HOSTILE");

	private String value;

	Identity() {
		this.value = Identity.values()[0].toString();
	}

	Identity(
			final String value ) {
		this.value = value;
	}

	public static Identity fromString(
			String value ) {
		for (final Identity item : Identity.values()) {
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
