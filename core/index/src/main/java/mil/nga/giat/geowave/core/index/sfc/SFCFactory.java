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
package mil.nga.giat.geowave.core.index.sfc;

import mil.nga.giat.geowave.core.index.sfc.hilbert.HilbertSFC;
import mil.nga.giat.geowave.core.index.sfc.xz.XZOrderSFC;
import mil.nga.giat.geowave.core.index.sfc.zorder.ZOrderSFC;

/***
 * Factory used to generate an instance of a known space filling curve type
 * 
 */
public class SFCFactory
{
	/***
	 * Generates a SFC instance based on the dimensions definition and the space
	 * filling curve type
	 * 
	 * @param dimensionDefs
	 *            specifies the min, max, and cardinality for this instance of
	 *            the SFC
	 * @param sfc
	 *            specifies the type (Hilbert, ZOrder) of space filling curve to
	 *            generate
	 * @return a space filling curve instance generated based on the supplied
	 *         parameters
	 */
	public static SpaceFillingCurve createSpaceFillingCurve(
			final SFCDimensionDefinition[] dimensionDefs,
			final SFCType sfc ) {

		switch (sfc) {
			case HILBERT:
				return new HilbertSFC(
						dimensionDefs);

			case ZORDER:
				return new ZOrderSFC(
						dimensionDefs);

			case XZORDER:
				return new XZOrderSFC(
						dimensionDefs);
		}

		return null;
	}

	/***
	 * Implemented and registered Space Filling curve types
	 * 
	 */
	public static enum SFCType {
		HILBERT,
		ZORDER,
		XZORDER
	}

}
