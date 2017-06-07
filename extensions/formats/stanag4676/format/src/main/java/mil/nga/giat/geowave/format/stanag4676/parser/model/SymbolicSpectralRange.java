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
 * Enumeration Provides image/video related feature data pertinent to a track.
 */
public enum SymbolicSpectralRange {
	/**
	 * Indicates Long-wavelength infrared (8 to 15 micrometers).
	 * <p>
	 * This is the "thermal imaging" region, in which sensors can obtain a
	 * completely passive picture of the outside world based on thermal
	 * emissions only and requiring no external light or thermal source such as
	 * the sun, moon or infrared illuminator.
	 */
	LWIR(
			"LWIR"),

	/**
	 * Indicates Mid-wavelength infrared (3 to 8 micrometers).
	 * <p>
	 * In guided missile technology the 3 to 5 micrometers portion of this band
	 * is the atmospheric window in which the homing heads of passive IR 'heat
	 * seeking' missiles are designed to work, homing on to the Infrared
	 * signature of the target aircraft, typically the jet engine exhaust plume.
	 */
	MWIR(
			"MWIR"),

	/**
	 * Indicates Short-wavelength infrared (1.4 to 3 micrometers).
	 * <p>
	 * Water absorption increases significantly at 1,450 nm. The 1,530 to 1,560
	 * nm range is the dominant spectral region for long-distance
	 * telecommunications.
	 */
	SWIR(
			"SWIR"),

	/**
	 * Indicates Near-infrared (75 to 1.4 micrometers) in wavelength
	 * <p>
	 * Defined by the water absorption, and commonly used in fiber optic
	 * telecommunication because of low attenuation losses in the SiO2 glass
	 * (silica) medium. Image intensifiers are sensitive to this area of the
	 * spectrum. Examples include night vision devices such as night vision
	 * goggles.
	 */
	NIR(
			"NIR"),

	/**
	 * Indicates portion of electromagnetic spectrum that is visible to (can be
	 * detected by) the human eye.
	 * <p>
	 * Visible light or simply light. A typical human eye will respond to
	 * wavelengths from about 390 to 750 nm.
	 */
	VIS(
			"VIS"),

	/**
	 * Indicates Ultraviolet (UV) light is electromagnetic radiation with a
	 * wavelength shorter than that of visible light, but longer than X-rays, in
	 * the range 10 to 400 nm.
	 */
	UV(
			"UV"),

	/**
	 * Indicates Multi-Spectral Imagery.
	 * <p>
	 * Contains imagery data comprising multiple spectral bands.
	 */
	MSI(
			"MSI"),

	/**
	 * Indicates Hyper-Spectral Imagery.
	 * <p>
	 * Contains imagery data comprising multiple spectral bands.
	 */
	HSI(
			"HSI"),

	/**
	 * Indicates the spectral band(s) are unknown.
	 */
	UNKNOWN(
			"UNKNOWN");

	private String value;

	SymbolicSpectralRange() {
		this.value = SymbolicSpectralRange.values()[0].toString();
	}

	SymbolicSpectralRange(
			final String value ) {
		this.value = value;
	}

	public static SymbolicSpectralRange fromString(
			String value ) {
		for (final SymbolicSpectralRange item : SymbolicSpectralRange.values()) {
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
