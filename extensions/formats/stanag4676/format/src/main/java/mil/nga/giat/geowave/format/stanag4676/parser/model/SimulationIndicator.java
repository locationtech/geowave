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
 * Enumeration an indication of whether reported information is real, simulated,
 * or synthesized.
 */
public enum SimulationIndicator {
	/**
	 * Actual sensor collected data
	 */
	REAL(
			"REAL"),

	/**
	 * Computer generated sensor data
	 */
	SIMULATED(
			"SIMULATED"),

	/**
	 * A combination of real and simulated data.
	 */
	SYNTHESIZED(
			"SYNTHESIZED");

	private String value;

	SimulationIndicator() {
		this.value = "REAL";
	}

	SimulationIndicator(
			final String value ) {
		this.value = value;
	}

	public static SimulationIndicator fromString(
			String value ) {
		for (final SimulationIndicator item : SimulationIndicator.values()) {
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
