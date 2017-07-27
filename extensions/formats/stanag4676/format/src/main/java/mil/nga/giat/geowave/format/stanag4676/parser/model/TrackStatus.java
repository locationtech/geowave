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

/**
 * Provides the status of a track (i.e. initiating, maintaining, dropping,
 * terminated).
 */
public enum TrackStatus {
	INITIATING(
			"INITIATING"),
	MAINTAINING(
			"MAINTAINING"),
	DROPPING(
			"DROPPING"),
	TERMINATED(
			"TERMINATED");

	private String value;

	TrackStatus() {
		this.value = TrackStatus.values()[0].toString();
	}

	TrackStatus(
			final String value ) {
		this.value = value;
	}

	public static TrackStatus fromString(
			String value ) {
		for (final TrackStatus item : TrackStatus.values()) {
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
