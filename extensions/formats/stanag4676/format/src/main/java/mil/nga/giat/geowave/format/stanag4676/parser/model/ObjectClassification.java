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
 * Enumeration for object classification
 */
public enum ObjectClassification {
	/**
	 * A wheeled vehicle
	 */
	WHEELED(
			"WHEELED"),

	/**
	 * A tracked vehicle
	 */
	TRACKED(
			"TRACKED"),

	/**
	 * A helicopter
	 */
	HELICOPTER(
			"HELICOPTER"),

	/**
	 * An Unmanned Aerial Vehicle
	 */
	UAV(
			"UAV"),

	/**
	 * A train
	 */
	TRAIN(
			"TRAIN"),

	/**
	 * A general aircraft
	 */
	AIRCRAFT(
			"AIRCRAFT"),

	/**
	 * A strike aircraft
	 */
	AIRCRAFT_STRIKE(
			"AIRCRAFT - STRIKE"),

	/**
	 * A transport aircraft
	 */
	AIRCRAFT_TRANSPORT(
			"AIRCRAFT - TRANSPORT"),

	/**
	 * A commercial aircraft
	 */
	AIRCRAFT_COMMERCIAL(
			"AIRCRAFT - COMMERCIAL"),

	/**
	 * A general watercraft
	 */
	WATERCRAFT(
			"WATERCRAFT"),

	/**
	 * A "go-fast" watercraft
	 */
	WATERCRAFT_GOFAST(
			"WATERCRAFT - GOFAST"),

	/**
	 * A pleasure watercraft
	 */
	WATERCRAFT_PLEASURE(
			"WATERCRAFT - PLEASURE"),

	/**
	 * A naval watercraft
	 */
	WATERCRAFT_NAVAL(
			"WATERCRAFT - NAVAL"),

	/**
	 * A cargo watercraft
	 */
	WATERCRAFT_CARGO(
			"WATERCRAFT - CARGO"),

	/**
	 * A car or sedan
	 */
	CAR(
			"CAR"),

	/**
	 * A motorcycle
	 */
	MOTORCYCLE(
			"MOTORCYCLE"),

	/**
	 * A "pickup" type truck
	 */
	TRUCK_PICKUP(
			"TRUCK - PICKUP"),

	/**
	 * A tractor-trailer type truck
	 */
	TRUCK_TRACTOR_TRAILER(
			"TRUCK - TRACTOR-TRAILER"),

	/**
	 * A box type truck
	 */
	TRUCK_BOX(
			"TRUCK - BOX"),

	/**
	 * A "Humvee" type truck
	 */
	TRUCK_HUMVEE(
			"TRUCK - HUMVEE"),

	/**
	 * An emergency vehicle
	 */
	EMERGENCY_VEHICLE(
			"EMERGENCY - VEHICLE"),

	/**
	 * A general dismount
	 */
	DISMOUNT(
			"DISMOUNT"),

	/**
	 * A combatant dismount
	 */
	DISMOUNT_COMBATANT(
			"DISMOUNT - COMBATANT"),

	/**
	 * A non-combatant dismount
	 */
	DISMOUNT_NONCOMBATANT(
			"DISMOUNT - NONCOMBATANT"),

	/**
	 * A male dismount
	 */
	DISMOUNT_MALE(
			"DISMOUNT - MALE"),

	/**
	 * A female dismount
	 */
	DISMOUNT_FEMALE(
			"DISMOUNT - FEMALE"),

	/**
	 * A group of dismounts
	 */
	DISMOUNT_GROUP(
			"DISMOUNT - GROUP");

	private String value;

	ObjectClassification() {
		this.value = ObjectClassification.values()[0].toString();
	}

	ObjectClassification(
			final String value ) {
		this.value = value;
	}

	public static ObjectClassification fromString(
			String value ) {
		for (final ObjectClassification item : ObjectClassification.values()) {
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
