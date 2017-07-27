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
 * Enumeration of TrackPointType
 */
public enum TrackPointType {
	/**
	 * A measured track point.
	 * <p>
	 * A detection marked as a track point, with no additional adjustments,
	 * automatic/machine filtering, or estimation processing (i.e."raw"
	 * detection information, or input to the tracker).
	 */
	MEASURED(
			"MEASURED"),

	/**
	 * A manual, estimated track point.
	 * <p>
	 * Position is approximated by an operator/analyst, based on one or more
	 * measurements and his/her analytical judgment (example: "snap to road").
	 */
	MANUAL_ESTIMATED(
			"MANUAL_ESTIMATED"),

	/**
	 * A manual, predicted track point.
	 * <p>
	 * A point provided by operator/analyst that is based on prior track
	 * history, but is not associated with a direct measurement.
	 */
	MANUAL_PREDICTED(
			"MANUAL_PREDICTED"),

	/**
	 * An automatic, estimated track point.
	 * <p>
	 * A point provided by automatic tracker, based on one or more measurements
	 * and automatic adjustments (example: "snap to road").
	 */
	AUTOMATIC_ESTIMATED(
			"AUTOMATIC_ESTIMATED"),

	/**
	 * An automatic, predicted track point.
	 * <p>
	 * A point provided by automatic tracker, based on prior track history, but
	 * is not associated with a direct measurement.
	 */
	AUTOMATIC_PREDICTED(
			"AUTOMATIC_PREDICTED");

	private String value;

	TrackPointType() {
		this.value = "MEASURED";
	}

	TrackPointType(
			final String value ) {
		this.value = value;
	}

	public static TrackPointType fromString(
			String value ) {
		for (final TrackPointType item : TrackPointType.values()) {
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
