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
			"AIRCRAFT_STRIKE"),

	/**
	 * A transport aircraft
	 */
	AIRCRAFT_TRANSPORT(
			"AIRCRAFT_TRANSPORT"),

	/**
	 * A commercial aircraft
	 */
	AIRCRAFT_COMMERCIAL(
			"AIRCRAFT_COMMERCIAL"),

	/**
	 * A general watercraft
	 */
	WATERCRAFT(
			"WATERCRAFT"),

	/**
	 * A "go-fast" watercraft
	 */
	WATERCRAFT_GOFAST(
			"WATERCRAFT_GOFAST"),

	/**
	 * A pleasure watercraft
	 */
	WATERCRAFT_PLEASURE(
			"WATERCRAFT_PLEASURE"),

	/**
	 * A naval watercraft
	 */
	WATERCRAFT_NAVAL(
			"WATERCRAFT_NAVAL"),

	/**
	 * A cargo watercraft
	 */
	WATERCRAFT_CARGO(
			"WATERCRAFT_CARGO"),

	/**
	 * A car or sedan
	 */
	CAR(
			"CAR"),

	/**
	 * A "pickup" type truck
	 */
	TRUCK_PICKUP(
			"TRUCK_PICKUP"),

	/**
	 * A tractor-trailer type truck
	 */
	TRUCK_TRACTOR_TRAILER(
			"TRUCK_TRACTOR_TRAILER"),

	/**
	 * A "Humvee" type truck
	 */
	TRUCK_HUMVEE(
			"TRUCK_HUMVEE"),

	/**
	 * An emergency vehicle
	 */
	EMERGENCY_VEHICLE(
			"EMERGENCY_VEHICLE"),

	/**
	 * A general dismount
	 */
	DISMOUNT(
			"DISMOUNT"),

	/**
	 * A combatant dismount
	 */
	DISMOUNT_COMBATANT(
			"DISMOUNT_COMBATANT"),

	/**
	 * A non-combatant dismount
	 */
	DISMOUNT_NONCOMBATANT(
			"DISMOUNT_NONCOMBATANT"),

	/**
	 * A male dismount
	 */
	DISMOUNT_MALE(
			"DISMOUNT_MALE"),

	/**
	 * A female dismount
	 */
	DISMOUNT_FEMALE(
			"DISMOUNT_FEMALE"),

	/**
	 * A group of dismounts
	 */
	DISMOUNT_GROUP(
			"DISMOUNT_GROUP");

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
