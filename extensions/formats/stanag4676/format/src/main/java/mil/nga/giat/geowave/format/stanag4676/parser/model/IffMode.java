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
 * Enumeration Provides mode of operation for an IFF system.
 */
public enum IffMode {
	/**
	 * Provides 2-digit 5-bit mission code. (military only, cockpit selectable)
	 */
	MODE1(
			"MODE1"),

	/**
	 * Provides 4-digit octal unit code. (military only, set on ground for
	 * fighters, can be changed in flight by transport aircraft)
	 */
	MODE2(
			"MODE2"),

	/**
	 * Shared with civilian secondary surveillance radar (SSR). Mode 3/A,
	 * provides a 4-digit octal identification code for the aircraft, assigned
	 * by the air traffic controller. (military and civilian)
	 */
	MODE3(
			"MODE3"),

	/**
	 * Provides a 3-pulse reply to crypto coded challenge. (military only).
	 * Modes 4 and 5 are designated for use by NATO forces:
	 */
	MODE4(
			"MODE4"),

	/**
	 * Provides a cryptographically secured version of Mode S and ADS-B GPS
	 * position. (military only). Mode 5 is divided into two levels. Both are
	 * crypto-secure with Enhanced encryption, Spread Spectrum Modulation, and
	 * Time of Day Authentication. Level 1 is similar to Mode 4 information but
	 * enhanced with an Aircraft Unique PIN. Level 2 is the same as Mode 5 level
	 * one but includes additional information such as Aircraft Position and
	 * Other. Modes 4 and 5 are designated for use by NATO forces
	 */
	MODE5(
			"MODE5"),

	/**
	 * Provides 4-digit octal code for aircraft's pressure altitude. (military
	 * and civilian)
	 */
	MODE_C(
			"MODE_C"),

	/**
	 * Provides multiple information formats to a selective interrogation. Each
	 * aircraft is assigned a fixed 24-bit address. (military and civilian)
	 */
	MODE_S(
			"MODE_S");

	private String value;

	IffMode() {
		this.value = IffMode.values()[0].toString();
	}

	IffMode(
			final String value ) {
		this.value = value;
	}

	public static IffMode fromString(
			String value ) {
		for (final IffMode item : IffMode.values()) {
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
