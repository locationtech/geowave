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
 * Enumeration Provides additional identity/status information (amplification)
 * of an object being tracked. Values in accordance with STANAG 1241.
 */
public enum IdentityAmplification {
	/**
	 * Per STANAG 1241 - friendly track, object or entity acting as exercise
	 * hostile
	 */
	FAKER(
			"FAKER"),

	/**
	 * Per STANAG 1241 - friendly track, object or entity acting as exercise
	 * suspect
	 */
	JOKER(
			"JOKER"),

	/**
	 * Per STANAG 1241 - friendly high value object. Can also be referred to as
	 * "special."
	 */
	KILO(
			"KILO"),

	/**
	 * Per STANAG 1241 - a suspect surface track following a recognized surface
	 * traffic route.
	 */
	TRAVELLER(
			"TRAVELLER"),

	/**
	 * Per STANAG 1241 - a suspect track, object or entity of special interest
	 */
	ZOMBIE(
			"ZOMBIE");

	private String value;

	IdentityAmplification() {
		this.value = IdentityAmplification.values()[0].toString();
	}

	IdentityAmplification(
			final String value ) {
		this.value = value;
	}

	public static IdentityAmplification fromString(
			String value ) {
		for (final IdentityAmplification item : IdentityAmplification.values()) {
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
