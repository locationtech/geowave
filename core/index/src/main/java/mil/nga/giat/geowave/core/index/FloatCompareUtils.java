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
package mil.nga.giat.geowave.core.index;

/**
 * Convenience methods for comparing floating point values.
 *
 */
public class FloatCompareUtils
{
	public static final double COMP_EPSILON = 2.22E-16;

	/**
	 * The == operator is not reliable for doubles, so we are using this method
	 * to check if two doubles are equal
	 * 
	 * @param x
	 * @param y
	 * @return true if the double are equal, false if they are not
	 */
	public static boolean checkDoublesEqual(
			double x,
			double y ) {
		boolean xNeg = false;
		boolean yNeg = false;
		double diff = (Math.abs(x) - Math.abs(y));

		if (x < 0.0) {
			xNeg = true;
		}
		if (y < 0.0) {
			yNeg = true;
		}
		return (diff <= COMP_EPSILON && diff >= -COMP_EPSILON && xNeg == yNeg);
	}

}
