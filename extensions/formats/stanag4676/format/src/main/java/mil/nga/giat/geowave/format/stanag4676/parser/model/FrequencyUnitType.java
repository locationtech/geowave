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
 * Enumeration for Frequency Unit
 */
public enum FrequencyUnitType {

	/**
	 * 1 Terra Hertz = 1,000,000,000,000 Hz (10^12)
	 */
	THz,

	/**
	 * 1 Giga Hertz = 1,000,000,000 Hz (10^9)
	 */
	GHz,

	/**
	 * 1 Mega Hertz = 1,000,000 Hz (10^6)
	 */
	MHz,

	/**
	 * 1 Kilo Hertz = 1,000 Hz (10^3)
	 */
	KHz,

	/**
	 * Hertz (Cycles per second)
	 */
	Hz;
}
