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
package mil.nga.giat.geowave.format.stanag4676.parser.util;

import java.io.Serializable;

import mil.nga.giat.geowave.core.index.FloatCompareUtils;

public class Length implements
		Serializable
{
	private static final long serialVersionUID = 1L;

	public static final double KMperNM = 1.852;
	public static final double KMperSM = 1.609344;
	public static final double FTperSM = 5280.0;
	public static final double SMperKM = 1.0 / KMperSM;
	public static final double FTperKM = SMperKM * FTperSM;
	public static final double INperKM = FTperKM * 12.0;
	public static final double YDperKM = FTperKM / 3.0;

	public enum LengthUnits {
		Kilometers,
		Meters,
		Decimeters,
		Centimeters,
		Millimeters,
		NauticalMiles,
		StatuteMiles,
		Yards,
		Feet,
		Inches
	}

	private double lengthKM;

	protected Length() {}

	private Length(
			final double lengthKM ) {
		this.lengthKM = lengthKM;
	}

	public static Double getKM(
			final Length length ) {
		return (length != null) ? length.getKM() : null;
	}

	public static Double getM(
			final Length length ) {
		return (length != null) ? length.getM() : null;
	}

	public static Length fromKM(
			final double lengthKM ) {
		return new Length(
				lengthKM);
	}

	public static Length fromNM(
			final double lengthNM ) {
		return new Length(
				lengthNM * KMperNM);
	}

	public static Length fromSM(
			final double lengthSM ) {
		return new Length(
				lengthSM * KMperSM);
	}

	public static Length fromM(
			final double lengthM ) {
		return new Length(
				lengthM / 1000);
	}

	public static Length fromDM(
			final double lengthDM ) {
		return Length.fromM(lengthDM / 10);
	}

	public static Length fromCM(
			final double lengthCM ) {
		return Length.fromM(lengthCM / 100);
	}

	public static Length fromMM(
			final double lengthMM ) {
		return Length.fromM(lengthMM / 1000);
	}

	public static Length fromFeet(
			final double lengthFeet ) {
		return new Length(
				lengthFeet / FTperKM);
	}

	public static Length fromYards(
			final double lengthYards ) {
		return new Length(
				lengthYards / YDperKM);
	}

	public static Length fromInches(
			final double lengthInches ) {
		return new Length(
				lengthInches / INperKM);
	}

	public static Length from(
			final LengthUnits units,
			final double val ) {
		switch (units) {
			case Kilometers:
				return fromKM(val);
			case Meters:
				return fromM(val);
			case Decimeters:
				return fromDM(val);
			case Centimeters:
				return fromCM(val);
			case Millimeters:
				return fromMM(val);
			case NauticalMiles:
				return fromNM(val);
			case StatuteMiles:
				return fromSM(val);
			case Feet:
				return fromFeet(val);
			case Yards:
				return fromYards(val);
			case Inches:
				return fromInches(val);
		}

		return fromKM(val);
	}

	// l1 + l2
	public final static Length add(
			final Length l1,
			final Length l2 ) {
		return Length.fromKM(l1.lengthKM + l2.lengthKM);
	}

	// l1 - l2
	public final static Length sub(
			final Length l1,
			final Length l2 ) {
		return Length.fromKM(l1.lengthKM - l2.lengthKM);
	}

	public final double getLength(
			final LengthUnits units ) {
		switch (units) {
			case Kilometers:
				return getKM();
			case Meters:
				return getM();
			case Decimeters:
				return getDM();
			case Centimeters:
				return getCM();
			case Millimeters:
				return getMM();
			case NauticalMiles:
				return getNM();
			case StatuteMiles:
				return getSM();
			case Feet:
				return getFeet();
			case Yards:
				return getYards();
			case Inches:
				return getInches();
		}

		return getKM();
	}

	public final double getKM() {
		return lengthKM;
	}

	public final double getM() {
		return lengthKM * 1000;
	}

	public final double getDM() {
		return lengthKM * 1000 * 10;
	}

	public final double getCM() {
		return lengthKM * 1000 * 100;
	}

	public final double getMM() {
		return lengthKM * 1000 * 1000;
	}

	public final double getNM() {
		return lengthKM / KMperNM;
	}

	public final double getSM() {
		return lengthKM / KMperSM;
	}

	public final double getFeet() {
		return lengthKM * FTperKM;
	}

	public final double getYards() {
		return lengthKM * YDperKM;
	}

	public final double getInches() {
		return lengthKM * INperKM;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (obj instanceof Length) {
			final Length objLength = (Length) obj;
			return FloatCompareUtils.checkDoublesEqual(
					objLength.lengthKM,
					lengthKM);
		}
		return false;
	}

	@Override
	public int hashCode() {
		final Double len = new Double(
				lengthKM);
		return len.hashCode();
	}

	@Override
	public final String toString() {
		return Double.toString(lengthKM) + "km";
	}
}
