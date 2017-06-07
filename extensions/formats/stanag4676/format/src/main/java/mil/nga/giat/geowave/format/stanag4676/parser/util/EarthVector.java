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

import javax.vecmath.AxisAngle4d;
import javax.vecmath.Matrix3d;
import javax.vecmath.Point2d;
import javax.vecmath.Vector3d;

import mil.nga.giat.geowave.core.index.FloatCompareUtils;

public class EarthVector
{

	public static final double X_EPSILON = 0.000000001; // degrees

	public static final double Y_EPSILON = 0.000001; // degrees

	public static final double ECF_EPSILON = 0.000000001;
	public static final int DEGREES = 0;
	public static final int RADIANS = 1;
	public static final double KMperNM = 1.852;
	public static final double KMperSM = 1.609344;
	public static final double FTperSM = 5280.0;
	public static final double SMperKM = 1.0 / KMperSM;
	public static final double FTperKM = SMperKM * FTperSM;
	public static final double INperKM = FTperKM * 12.0;
	public static final double YDperKM = FTperKM / 3.0;
	public static final double KMperDegree = 111.12;
	public static final double NMperDegree = 60.0;
	public static final double REKM = 6378.137;
	public static final double CEKM = REKM * Math.PI * 2.0;
	public static final double RENM = 3443.918466523;
	public static final double POLAR_RENM = 3432.371659977;
	public static final double FLATTENING_FACTOR = (POLAR_RENM / RENM);
	public static final double EARTH_FLATTENING = (1.0 / 298.257224);
	public static final double FLAT_COEFF1 = (EARTH_FLATTENING * (2.0 - EARTH_FLATTENING));
	public static final double FLAT_COEFF2 = (1.0 - FLAT_COEFF1);
	public static final double DPR = 57.29577951308232;
	public static final double RAD_1 = 0.0174532925199433;
	public static final double RAD_45 = 0.785398163;
	public static final double RAD_90 = 1.57079632679489661923;
	public static final double RAD_180 = RAD_90 * 2.0;
	public static final double RAD_270 = RAD_90 * 3.0;
	public static final double RAD_360 = RAD_90 * 4.0;
	public static final double EARTH_ROT_RATE = 7.2921158553e-5;
	public static final double G = 9.8066e-3;
	public static final double GM = 3.98600800e5;
	public static final double J2 = 1.082630e-3;
	public static final double J3 = -2.532152e-6;
	public static final double J4 = -1.610988e-6;
	public static final double J5 = -2.357857e-7;
	public static final int SEC_90 = 324000;
	public static final int SEC_180 = SEC_90 * 2;
	public static final int SEC_270 = SEC_90 * 3;
	public static final int SEC_360 = SEC_90 * 4;

	// Members
	protected double latitude;
	protected double longitude;
	protected double elevation;
	protected Vector3d ecfVector;
	protected boolean oblate = false;

	/**
	 * Factory methods
	 */
	public static EarthVector fromDegrees(
			final double lat,
			final double lon ) {
		return new EarthVector(
				lat,
				lon,
				DEGREES);
	}

	public static EarthVector translateDegrees(
			final double lat,
			final double lon,
			final Vector3d translation ) {
		final EarthVector result = EarthVector.fromDegrees(
				lat,
				lon);
		result.getVector().add(
				translation);
		return new EarthVector(
				result.getVector());
	}

	/**
	 * Default constructor
	 */
	public EarthVector() {
		latitude = 0.0;
		longitude = 0.0;
		elevation = 0.0;

		initVector();
	}

	/**
	 * lat/long (radians) constructor
	 */
	public EarthVector(
			final double inlat,
			final double inlon ) {
		latitude = inlat;
		longitude = inlon;
		elevation = 0.0;

		initVector();
	}

	/**
	 * lat/long (radians or degrees) constructor
	 */
	public EarthVector(
			final double inlat,
			final double inlon,
			final int units ) {
		if (units == DEGREES) {
			latitude = degToRad(inlat);
			longitude = degToRad(inlon);
		}
		else {
			latitude = inlat;
			longitude = inlon;
		}
		elevation = 0.0;

		initVector();
	}

	/**
	 * lat/long/elev (radians) constructor
	 */
	public EarthVector(
			final double inlat,
			final double inlon,
			final double inelev ) {
		latitude = inlat;
		longitude = inlon;
		elevation = inelev;

		initVector();
	}

	/**
	 * lat/long/elev (radians or degrees) constructor
	 */
	public EarthVector(
			final double inlat,
			final double inlon,
			final double inelev,
			final int units ) {
		if (units == DEGREES) {
			latitude = degToRad(inlat);
			longitude = degToRad(inlon);
		}
		else {
			latitude = inlat;
			longitude = inlon;
		}
		elevation = inelev;

		initVector();
	}

	/**
	 * Point2d (radians) constructor
	 */
	public EarthVector(
			final Point2d point ) {
		latitude = point.y;
		longitude = point.x;
		elevation = 0.0;

		initVector();
	}

	/**
	 * Point2d (degrees or radians) constructor
	 */
	public EarthVector(
			final Point2d point,
			final int units ) {
		if (units == DEGREES) {
			latitude = degToRad(point.y);
			longitude = degToRad(point.x);
		}
		else {
			latitude = point.y;
			longitude = point.x;
		}
		elevation = 0.0;

		initVector();
	}

	/**
	 * Point2d/elev (radians) constructor
	 */
	public EarthVector(
			final Point2d point,
			final double inelev ) {
		latitude = point.y;
		longitude = point.x;
		elevation = inelev;

		initVector();
	}

	/**
	 * Point2d/elev (degrees or radians) constructor
	 */
	public EarthVector(
			final Point2d point,
			final double inelev,
			final int units ) {
		if (units == DEGREES) {
			latitude = degToRad(point.y);
			longitude = degToRad(point.x);
		}
		else {
			latitude = point.y;
			longitude = point.x;
		}
		elevation = inelev;

		initVector();
	}

	/**
	 * Vector3d (ECF or unit vector) constructor If vector is ECF, elevation is
	 * derived from it Otherwise, elevation is zero
	 */
	public EarthVector(
			final Vector3d vec ) {
		final Vector3d norm = new Vector3d(
				vec);
		norm.normalize();

		final double sinlat = norm.z;
		final double coslat = Math.sqrt(Math.abs(1.0 - (sinlat * sinlat)));
		latitude = Math.atan2(
				sinlat,
				coslat);

		double vra;
		// not sure which epsilon value is most appropriate - just chose Y eps.
		// because it's bigger
		if (Math.abs(coslat) <= Y_EPSILON) {
			// this value's kind of arbitrary in this case
			vra = 0.0;
		}
		else {
			// this unchecked divide by 0 was causing EV's to have NaN's and
			// such
			// sometimes, causing stuff to break, especially for the globe view
			final double cosa = norm.x / coslat;
			final double sina = norm.y / coslat;

			if (Math.abs(cosa) < X_EPSILON) {
				vra = RAD_90 * sign(sina);
			}
			else {
				vra = Math.atan2(
						sina,
						cosa);
			}
		}

		longitude = vra;

		if (vec.length() > getEarthRadiusKM()) {
			elevation = vec.length() - getEarthRadiusKM();
		}
		else {
			elevation = 0.0;
		}

		initVector();
	}

	/**
	 * Vector3d (unit vector)/elev constructor
	 */
	public EarthVector(
			final Vector3d vec,
			final double inelev ) {
		final Vector3d norm = vec;
		norm.normalize();

		final double sinlat = norm.z;
		final double coslat = Math.sqrt(Math.abs(1.0 - (sinlat * sinlat)));
		latitude = Math.atan2(
				sinlat,
				coslat);

		final double cosa = norm.x / coslat;
		final double sina = norm.y / coslat;
		double vra;

		if (Math.abs(cosa) < 0.001) {
			vra = RAD_90 * sign(sina);
		}
		else {
			vra = Math.atan2(
					sina,
					cosa);
		}

		longitude = vra;

		elevation = inelev;

		initVector();
	}

	/**
	 * EarthVector (copy) constructor
	 */
	public EarthVector(
			final EarthVector loc ) {
		if (loc == null) {
			latitude = 0.0;
			longitude = 0.0;
			elevation = 0.0;
		}
		else {
			latitude = loc.getLatitude();
			longitude = loc.getLongitude();
			elevation = loc.getElevation();
		}

		initVector();
	}

	/**
	 * Copy the input coordinate
	 */
	public void setCoord(
			final EarthVector coord ) {
		latitude = coord.getLatitude();
		longitude = coord.getLongitude();
		elevation = coord.getElevation();

		initVector();
	}

	/**
	 * equals - compare ecf position (x,y,z) for equality with an epsilon value
	 */
	public boolean epsilonEquals(
			final EarthVector otherEv,
			final double epsilon ) {
		if (otherEv == null) {
			return false;
		}
		return ecfVector.epsilonEquals(
				otherEv.ecfVector,
				epsilon);
	}

	/**
	 * equals - compare ecf position (x,y,z) for equality
	 */
	@Override
	public boolean equals(
			final Object obj ) {
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof EarthVector)) {
			return false;
		}
		final EarthVector coord = (EarthVector) obj;

		return (FloatCompareUtils.checkDoublesEqual(
				coord.getX(),
				ecfVector.x) && FloatCompareUtils.checkDoublesEqual(
				coord.getY(),
				ecfVector.y) && FloatCompareUtils.checkDoublesEqual(
				coord.getZ(),
				ecfVector.z));
	}

	@Override
	public int hashCode() {
		return ecfVector.hashCode();
	}

	/**
	 * Initialize the internal ECF vector from radian lat/long
	 */
	protected void initVector() {
		// normalizeInputs();

		final double x = Math.cos(latitude) * Math.cos(longitude) * getRadius();
		final double y = Math.cos(latitude) * Math.sin(longitude) * getRadius();
		double z;
		final double sin_lat = Math.sin(latitude);

		if (oblate) {
			final double ann = REKM / Math.sqrt(1.0 - (FLAT_COEFF1 * sin_lat * sin_lat));
			z = (sin_lat * (ann * FLAT_COEFF2));
		}
		else {
			z = sin_lat * getRadius();
		}

		ecfVector = new Vector3d(
				x,
				y,
				z);
	}

	public static double normalizeLongitude(
			final double lon ) {
		double nLon = lon;

		if (nLon > 180) {
			nLon -= 360;
		}
		else if (nLon < -180) {
			nLon += 360;
		}

		return nLon;
	}

	public static double normalizeLatitude(
			final double lat ) {
		double nLat = lat;

		if (nLat > 90) {
			nLat = 90 - (nLat % 90);
		}
		else if (nLat < -90) {
			nLat = -90 + (Math.abs(nLat) % 90);
		}

		return nLat;
	}

	/**
	 * Set/get oblateness
	 */
	public void setOblate(
			final boolean isOblate ) {
		oblate = isOblate;
	}

	public boolean isOblate() {
		return oblate;
	}

	/**
	 * Convert degrees to radians
	 */
	public static double degToRad(
			final double deg ) {
		return deg * RAD_1;
	}

	/**
	 * Convert radians to degrees
	 */
	public static double radToDeg(
			final double rad ) {
		double deg = rad * DPR;

		// normalize to (-180 to 180)
		while (deg > 180) {
			deg -= 360;
		}
		while (deg < -180) {
			deg += 360;
		}

		return deg;
	}

	/**
	 * Convert kilometers to nautical miles
	 */
	public static double KMToNM(
			final double km ) {
		return (km / KMperNM);
	}

	/**
	 * Convert kilometers to statute miles
	 */
	public static double KMToSM(
			final double km ) {
		return (km / KMperSM);
	}

	/**
	 * Convert nautical miles to kilometers
	 */
	public static double NMToKM(
			final double nm ) {
		return (nm * KMperNM);
	}

	/**
	 * Convert statute miles to kilometers
	 */
	public static double SMToKM(
			final double sm ) {
		return (sm * KMperSM);
	}

	/**
	 * Get the latitude (radians)
	 */
	public double getLatitude() {
		return latitude;
	}

	/**
	 * Get the latitude (degrees or radians)
	 */
	public double getLatitude(
			final int units ) {
		if (units == DEGREES) {
			return radToDeg(latitude);
		}
		else {
			return latitude;
		}
	}

	/**
	 * Get the longitude (radians)
	 */
	public double getLongitude() {
		return longitude;
	}

	/**
	 * Get the longitude (degrees or radians)
	 */
	public double getLongitude(
			final int units ) {
		if (units == DEGREES) {
			return radToDeg(longitude);
		}
		else {
			return longitude;
		}
	}

	/**
	 * Get the elevation (km)
	 */
	public double getElevation() {
		return elevation;
	}

	/**
	 * Set the latitude (radians)
	 */
	public void setLatitude(
			final double inlat ) {
		latitude = inlat;
		initVector();
	}

	/**
	 * Set the latitude (degrees or radians)
	 */
	public void setLatitude(
			final double inlat,
			final int units ) {
		if (units == DEGREES) {
			latitude = degToRad(inlat);
		}
		else {
			latitude = inlat;
		}

		initVector();
	}

	/**
	 * Set the longitude (radians)
	 */
	public void setLongitude(
			final double inlon ) {
		longitude = inlon;
		initVector();
	}

	/**
	 * Set the longitude (degrees or radians)
	 */
	public void setLongitude(
			final double inlon,
			final int units ) {
		if (units == DEGREES) {
			longitude = degToRad(inlon);
		}
		else {
			longitude = inlon;
		}

		initVector();
	}

	/**
	 * Set the elevation (km)
	 */
	public void setElevation(
			final double inelev ) {
		elevation = inelev;
		initVector();
	}

	/**
	 * Return the ECF vector
	 */
	public Vector3d getVector() {
		return ecfVector;
	}

	/**
	 * Copy the ECF vector
	 */
	public EarthVector setVector(
			final Vector3d vec ) {
		final EarthVector loc = new EarthVector(
				vec);
		latitude = loc.getLatitude();
		longitude = loc.getLongitude();
		elevation = loc.getElevation();
		ecfVector = loc.getVector();

		return this;
	}

	/**
	 * Get the x coordinate of the ECF vector (km)
	 */
	public double getX() {
		return (ecfVector.x);
	}

	/**
	 * Get the y coordinate of the ECF vector (km)
	 */
	public double getY() {
		return (ecfVector.y);
	}

	/**
	 * Get the z coordinate of the ECF vector (km)
	 */
	public double getZ() {
		return (ecfVector.z);
	}

	/**
	 * Normalize the ECF vector
	 */
	public Vector3d getUnitVector() {
		final Vector3d unitVec = new Vector3d(
				ecfVector);
		unitVec.normalize();

		return unitVec;
	}

	/**
	 * Create a great circle from this point to an endpoint
	 */
	public EarthVector[] makeGreatCircle(
			final EarthVector endpoint ) {
		return makeGreatCircleSegmentLength(
				endpoint,
				100);
	}

	public EarthVector[] makeGreatCircleSegmentLength(
			final EarthVector endpoint,
			final double segmentLengthKM ) {
		final int segments = getNumGreatCircleSegments(
				endpoint,
				segmentLengthKM,
				false);
		return makeGreatCircleNumSegments(
				endpoint,
				segments,
				false);
	}

	public EarthVector[] makeGreatCircleSegmentLengthReverseDirection(
			final EarthVector endpoint,
			final double segmentLengthKM ) {
		final int segments = getNumGreatCircleSegments(
				endpoint,
				segmentLengthKM,
				true);
		return makeGreatCircleNumSegments(
				endpoint,
				segments,
				true);
	}

	public EarthVector[] makeGreatCircleNumSegments(
			final EarthVector endpoint,
			final int segments ) {
		return makeGreatCircleNumSegments(
				endpoint,
				segments,
				false);
	}

	public EarthVector[] makeGreatCircleNumSegments(
			final EarthVector endpoint,
			final int segments,
			final boolean reverseDirection ) {
		final double resolution = 1.0 / segments;
		double fraction = 0;

		final EarthVector points[] = new EarthVector[segments + 1];
		points[0] = new EarthVector(
				this);
		points[segments] = new EarthVector(
				endpoint);

		if (reverseDirection) {
			final double reverseFactor = getDistanceReverseDirection(endpoint) / getDistance(endpoint);
			for (int i = 1; i < segments; i++) {
				fraction = i * resolution;
				points[i] = findPointReverseDirection(
						endpoint,
						fraction * reverseFactor);

			}

		}
		else {
			for (int i = 1; i < segments; i++) {
				fraction = i * resolution;
				points[i] = this.findPoint(
						endpoint,
						fraction);
			}
		}
		return points;
	}

	public EarthVector[] makeInterpolatedLineSegmentLength(
			final EarthVector endpoint,
			final double segmentLengthKM ) {
		return makeInterpolatedLineSegmentLength(
				endpoint,
				segmentLengthKM,
				false);
	}

	public EarthVector[] makeInterpolatedLineSegmentLength(
			final EarthVector endpoint,
			final double segmentLengthKM,
			final boolean reverseDirection ) {
		final int segments = getNumGreatCircleSegments(
				endpoint,
				segmentLengthKM,
				reverseDirection);
		return makeInterpolatedLineNumSegments(
				endpoint,
				segments,
				reverseDirection);
	}

	public EarthVector[] makeInterpolatedLineNumSegments(
			final EarthVector endpoint,
			final int segments ) {
		return makeInterpolatedLineNumSegments(
				endpoint,
				segments,
				false);
	}

	public EarthVector[] makeInterpolatedLineNumSegments(
			final EarthVector endpoint,
			final int segments,
			final boolean reverseDirection ) {
		final double resolution = 1.0 / segments;
		final EarthVector points[] = new EarthVector[segments + 1];

		points[0] = new EarthVector(
				this);
		points[segments] = new EarthVector(
				endpoint);

		final double baseLat = points[0].getLatitude(EarthVector.DEGREES);
		final double latStep = (points[segments].getLatitude(EarthVector.DEGREES) - baseLat) * resolution;
		final double baseLon = points[0].getLongitude(EarthVector.DEGREES);
		double deltaLon = (points[segments].getLongitude(EarthVector.DEGREES) - baseLon);
		final double baseAlt = points[0].elevation;
		final double altStep = (points[segments].elevation - baseAlt) * resolution;
		if (Math.abs(deltaLon) >= 0.0) {
			if (reverseDirection) {
				if (Math.abs(deltaLon) < 180) {
					// reverse it
					if (deltaLon > 0) {
						deltaLon = deltaLon - 360;
					}
					else {
						deltaLon = deltaLon + 360;
					}
				}
				// otherwise leave it alone
			}
			else {
				if (deltaLon > 180) {
					// reverse it
					deltaLon = -360 + deltaLon;
				}
				else if (deltaLon < -180) {
					// reverse it
					deltaLon = 360 + deltaLon;
				}
				// otherwise leave it alone
			}
		}

		final double lonStep = deltaLon * resolution;
		for (int i = 1; i < segments; i++) {
			final double lat = baseLat + (i * latStep);
			final double lon = get180NormalizedLon(baseLon + (i * lonStep));
			final double alt = baseAlt + (i * altStep);

			points[i] = new EarthVector(
					degToRad(lat),
					degToRad(lon),
					alt);
		}

		return points;
	}

	static private double get180NormalizedLon(
			final double lon ) {
		double newLon = lon;
		while (newLon < -180) {
			newLon += 360;
		}
		while (newLon > 180) {
			newLon -= 360;
		}
		return newLon;
	}

	public int getNumGreatCircleSegments(
			final EarthVector endpoint,
			final double segmentLengthKM ) {
		return getNumGreatCircleSegments(
				endpoint,
				segmentLengthKM,
				false);
	}

	public int getNumGreatCircleSegments(
			final EarthVector endpoint,
			double segmentLengthKM,
			final boolean reverseDirection ) {
		if (segmentLengthKM <= 0) {
			segmentLengthKM = 100;
		}

		// If the line is longer than maxSegmentLength, break it up into
		// smaller segments that follow a great circle arc.
		double distance;
		if (reverseDirection) {
			distance = getDistanceReverseDirection(endpoint);
		}
		else {
			distance = getDistance(endpoint);
		}

		return (int) (distance / segmentLengthKM) + 1;
	}

	/**
	 * Locate a coordinate on the line between this one and the "next" coord, at
	 * some fraction of the distance between them
	 */
	public EarthVector findPoint(
			final EarthVector nextCoord,
			final double fraction ) {
		// check for same point first
		if (equals(nextCoord)) {
			return new EarthVector(
					this);
		}

		// compute the vector normal to this vector and the input vector
		final Vector3d nextVector = nextCoord.getVector();
		final Vector3d vec = new Vector3d();
		vec.cross(
				ecfVector,
				nextVector);

		// compute the fractional angle between this vector and the input vector
		final double phi = fraction * Math.acos(ecfVector.dot(nextVector) / (ecfVector.length() * nextVector.length()));

		// create the vector rotated through phi about the normal vector
		final Vector3d output = rotate(
				vec,
				phi);

		// now scale the output vector by interpolating the magnitudes
		// of this vector and the input vector
		output.normalize();
		final double size = ((nextVector.length() - ecfVector.length()) * fraction) + ecfVector.length();
		output.scale(size);

		return new EarthVector(
				output);
	}

	public EarthVector findPointReverseDirection(
			final EarthVector nextCoord,
			final double fraction ) {
		// check for same point first
		if (equals(nextCoord)) {
			return new EarthVector(
					this);
		}

		// compute the vector normal to this vector and the input vector
		final Vector3d nextVector = nextCoord.getVector();
		final Vector3d vec = new Vector3d();
		vec.cross(
				ecfVector,
				nextVector);
		vec.negate();

		// compute the fractional angle between this vector and the input vector
		final double phi = fraction * Math.acos(ecfVector.dot(nextVector) / (ecfVector.length() * nextVector.length()));

		// create the vector rotated through phi about the normal vector
		final Vector3d output = rotate(
				vec,
				phi);
		// now scale the output vector by interpolating the magnitudes
		// of this vector and the input vector
		output.normalize();
		final double size = ((nextVector.length() - ecfVector.length()) * fraction) + ecfVector.length();
		output.scale(size);

		return new EarthVector(
				output);
	}

	public Vector3d getNormalizedEarthTangentVector(
			final double azimuth ) {
		// TODO: rewrite this to use real math instead of this silly difference

		final EarthVector nextEV = findPoint(
				1,
				azimuth);

		final Vector3d deltaVec = new Vector3d();
		deltaVec.sub(
				nextEV.getVector(),
				getVector());

		deltaVec.normalize();

		return deltaVec;
	}

	/**
	 * Locate a coordinate at a specific distance (km) and heading (radians)
	 * from this one.
	 */
	public EarthVector findPoint(
			final double distanceKM,
			final double azimuth ) {
		// initialize output location to this location
		final EarthVector locNorth = new EarthVector(
				this);

		// convert distance to radians
		final double distR = distanceKM / kmPerDegree() / DPR;

		// add converted distance to the origin latitude (ie, due north)
		locNorth.setLatitude(locNorth.getLatitude() + distR);

		// be careful! we might go over the pole
		if (locNorth.getLatitude() > RAD_90) {
			locNorth.setLatitude(RAD_180 - locNorth.getLatitude());
			locNorth.setLongitude(locNorth.getLongitude() + RAD_180);
		}

		// rotate the point due north around the origin to the new azimuth
		final Vector3d vec = locNorth.rotate(
				ecfVector,
				-azimuth);

		// retain the elevation from this coordinate
		final EarthVector newPoint = new EarthVector(
				vec);
		newPoint.setElevation(getElevation());

		return (newPoint);
	}

	/**
	 * Locate a coordinate at a specific distance (km), elevation angle
	 * (radians), and heading (radians) from this one.
	 */
	public EarthVector findPoint(
			final double distanceKM,
			final double azimuth,
			final double elevAngle ) {
		// convert distance to radians
		// final double distR = distanceKM / KMPerDegree() / DPR;
		final double lon = getLongitude();
		final double lat = getLatitude();
		// convert local enu to ecf to get east and north vectors
		// east vector
		final Vector3d eastVec = new Vector3d(
				1,
				0,
				0);
		final Vector3d northVec = new Vector3d(
				0,
				1,
				0);
		final double sinLon = Math.sin(lon);
		final double cosLon = Math.cos(lon);
		final double sinLat = Math.sin(lat);
		final double cosLat = Math.cos(lat);
		final Matrix3d enuToEcf = new Matrix3d();
		enuToEcf.m00 = -sinLon;
		enuToEcf.m01 = -(sinLat * cosLon);
		enuToEcf.m02 = cosLat * cosLon;
		enuToEcf.m10 = cosLon;
		enuToEcf.m11 = -(sinLat * sinLon);
		enuToEcf.m12 = cosLat * sinLon;
		enuToEcf.m20 = 0;
		enuToEcf.m21 = cosLat;
		enuToEcf.m22 = sinLat;
		enuToEcf.transform(eastVec);
		enuToEcf.transform(northVec);
		eastVec.normalize();
		northVec.normalize();
		northVec.scale(distanceKM);
		final Matrix3d elevTrans = new Matrix3d();
		elevTrans.set(new AxisAngle4d(
				eastVec,
				elevAngle));

		elevTrans.transform(northVec);
		final Matrix3d azTrans = new Matrix3d();
		final Vector3d unitEcf = new Vector3d(
				ecfVector);
		unitEcf.normalize();
		azTrans.set(new AxisAngle4d(
				unitEcf,
				azimuth));
		azTrans.transform(northVec);
		final Vector3d transformedEcf = new Vector3d();
		transformedEcf.add(
				ecfVector,
				northVec);
		final EarthVector transformedEv = new EarthVector(
				transformedEcf);
		return transformedEv;
	}

	public static double kmToRadians(
			final double distKM,
			final double latRad ) {
		return distKM / kmPerDegree(latRad) / DPR;
	}

	public static double kmToDegrees(
			final double distKM,
			final double latDeg ) {
		return distKM / kmPerDegree(latDeg / DPR);
	}

	public static double radianstoKM(
			final double distRad,
			final double latRad ) {
		return distRad * kmPerDegree(latRad) * DPR;
	}

	public static double degreestoKM(
			final double distDeg,
			final double latDeg ) {
		return distDeg * kmPerDegree(latDeg / DPR);
	}

	public double kmToRadians(
			final double distKM ) {
		return distKM / kmPerDegree() / DPR;
	}

	public double kmToDegrees(
			final double distKM ) {
		return distKM / kmPerDegree();
	}

	/**
	 * Rotates this coordinate about the input vector through the input angle
	 * (radians - because we usually use this internally)
	 * 
	 * @param vecAxis
	 *            The axis of rotation
	 * @param ang
	 *            The angle of rotation (in radians)
	 */
	// public Vector3d rotate( Vector3d vecAxis, double ang )
	// {
	// Vector3d vec1 = new Vector3d(vecAxis);
	// vec1.normalize();
	// Vector3d vec2 = new Vector3d();
	// vec2.cross(vec1, ecfVector);
	// Vector3d vec3 = new Vector3d();
	// vec3.cross(vec2, vec1);
	//
	// double ang_sin = Math.sin(ang);
	// double ang_cos = Math.cos(ang) - 1.0;
	//
	// Vector3d result = new Vector3d();
	// result.x = ecfVector.x + ang_cos*vec3.x + ang_sin*vec2.x;
	// result.y = ecfVector.y + ang_cos*vec3.y + ang_sin*vec2.y;
	// result.z = ecfVector.z + ang_cos*vec3.z + ang_sin*vec2.z;
	//
	// return result;
	// }
	public Vector3d rotate(
			final Vector3d rotAxis,
			final double angle ) {
		final Vector3d thisVec = new Vector3d(
				ecfVector);
		final Vector3d axis = new Vector3d(
				rotAxis);
		axis.normalize();

		final Matrix3d trans = new Matrix3d();
		trans.set(new AxisAngle4d(
				axis,
				angle));

		trans.transform(thisVec);

		return thisVec;
	}

	public double getVectorDistanceKMSq(
			final EarthVector loc ) {
		final Vector3d delta = getVector(loc);

		return delta.lengthSquared();
	}

	/**
	 * Compute the distance (km) from this coord to the input coord using vector
	 * math (my personal favorite)
	 * 
	 * @param loc
	 *            The coordinate to compute the distance to
	 */
	public double getDistance(
			final EarthVector loc ) {
		double dist = getEarthRadiusKM()
				* (Math.acos(ecfVector.dot(loc.getVector()) / (ecfVector.length() * loc.getVector().length())));

		if (Double.isNaN(dist) || Double.isInfinite(dist)) {
			dist = 0;
		}

		return dist;
	}

	/**
	 * Compute the distance (km) from this coord to the input coord using vector
	 * math (my personal favorite)
	 * 
	 * @param loc
	 *            The coordinate to compute the distance to
	 */
	public double getDistanceReverseDirection(
			final EarthVector loc ) {
		double dist = getEarthRadiusKM()
				* ((2 * Math.PI) - Math.acos(ecfVector.dot(loc.getVector())
						/ (ecfVector.length() * loc.getVector().length())));

		if (Double.isNaN(dist) || Double.isInfinite(dist)) {
			dist = 0;
		}

		return dist;
	}

	/**
	 * Compute the distance (km) from this coord to the input coord using
	 * trigonometry.
	 * 
	 * @param loc
	 *            The coordinate to compute the distance to
	 */
	public double getSphereDistance(
			final EarthVector loc ) {
		return (getEarthRadiusKM() * (Math.acos((Math.sin(latitude) * Math.sin(loc.getLatitude()))
				+ (Math.cos(latitude) * Math.cos(loc.getLatitude()) * Math.cos(loc.getLongitude() - longitude)))));
	}

	/**
	 * Compute the azimuth (in radians) from this coord to the input coord
	 * 
	 * @param loc
	 *            The coordinate to compute the distance to
	 */
	public double getAzimuth(
			final EarthVector loc ) {
		final EarthVector thisNorm = new EarthVector(
				this);
		thisNorm.setElevation(0);
		final EarthVector otherNorm = new EarthVector(
				loc);
		otherNorm.setElevation(0);
		return thisNorm.internalGetAzimuth(otherNorm);
	}

	private double internalGetAzimuth(
			final EarthVector loc ) { // Calculate the True North unit vector
		final EarthVector locNorth = new EarthVector(
				this);
		final double radInc = Math.max(
				RAD_1,
				Math.abs(loc.getLatitude() - getLatitude()));
		final boolean calcNorth = (latitude < loc.getLatitude());
		if (calcNorth) {
			locNorth.setLatitude(locNorth.getLatitude() + radInc);
		}
		else {
			locNorth.setLatitude(locNorth.getLatitude() - radInc);
		}
		final Vector3d vecNorth = locNorth.getVector();
		vecNorth.sub(ecfVector);

		// Calculate the azimuth between this and loc
		final Vector3d vecTemp = new Vector3d(
				loc.getVector());
		vecTemp.sub(ecfVector);

		vecNorth.normalize();
		vecTemp.normalize();
		double azimuth = Math.acos(vecNorth.dot(vecTemp));
		if (!calcNorth) {
			azimuth = RAD_180 - azimuth;
		}
		final double deltaLon = Math.abs(loc.getLongitude() - longitude);
		if (((loc.getLongitude() < longitude) && (deltaLon < RAD_180))
				|| ((loc.getLongitude() > longitude) && (deltaLon > RAD_180))) {
			// normalize azimuth to 0-360 degrees
			azimuth = RAD_360 - azimuth;
		}

		return azimuth;
	}

	/**
	 * Compute the vector from this coord to the input coord
	 * 
	 * @param loc
	 * @return
	 */
	public Vector3d getVector(
			final EarthVector loc ) {
		final Vector3d vecTemp = new Vector3d(
				loc.getVector());
		vecTemp.sub(ecfVector);

		return vecTemp;
	}

	/**
	 * Compute the radius (km) from the origin to this coord
	 */
	public double getRadius() {
		return (elevation + getEarthRadiusKM());
	}

	/**
	 * Retrieve the radius of the earth (km) at this coordinate's latitude
	 */
	public double getEarthRadiusKM() {
		return getEarthRadiusKM(
				latitude,
				oblate);
	}

	/**
	 * Retrieve the radius of the earth (km) statically for the given latitude
	 */
	public static double getEarthRadiusKM(
			final double lat,
			final boolean flat ) {
		final double radiusAtEquatorKM = REKM;

		if (flat) {
			return ((radiusAtEquatorKM * (1.0 - EARTH_FLATTENING)) / Math.sqrt(1.0 - (Math.cos(lat) * Math.cos(lat)
					* EARTH_FLATTENING * (2.0 - EARTH_FLATTENING))));
		}
		else {
			return radiusAtEquatorKM;
		}
	}

	/**
	 * Retrieve the number of kilometers per degree at the given latitude
	 */
	public static double kmPerDegree(
			final double lat ) {
		return ((RAD_360 * getEarthRadiusKM(
				lat,
				false)) / 360.0);
	}

	/**
	 * Retrieve the number of kilometers per degree for this coord's latitude
	 */
	public double kmPerDegree() {
		return ((RAD_360 * getEarthRadiusKM()) / 360.0);
	}

	/**
	 * return the sign of the argument
	 */
	protected double sign(
			final double x ) {
		if (x < 0.0) {
			return (-1.0);
		}
		else if (x > 0.0) {
			return (1.0);
		}
		else {
			return (0.0);
		}
	}

	@Override
	public String toString() {
		return getLatitude(DEGREES) + ", " + getLongitude(DEGREES);
	}

	public Point2d getPoint2d() {
		return new Point2d(
				getLongitude(DEGREES),
				getLatitude(DEGREES));
	}
}
