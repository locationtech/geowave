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
package mil.nga.giat.geowave.analytic.extract;

import java.util.Calendar;
import java.util.Date;

import mil.nga.giat.geowave.core.geotime.TimeUtils;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * 
 * A default implementation that averages all time attributes.
 * 
 */
public class TimeDimensionExtractor extends
		SimpleFeatureGeometryExtractor implements
		DimensionExtractor<SimpleFeature>
{
	private static final String[] TIME_NAME = new String[] {
		"time"
	};

	@Override
	public double[] getDimensions(
			SimpleFeature anObject ) {
		double[] timeVal = new double[1];
		double count = 0.0;
		for (AttributeDescriptor attr : anObject.getFeatureType().getAttributeDescriptors()) {
			if (TimeUtils.isTemporal(attr.getType().getClass())) {
				Object o = anObject.getAttribute(attr.getName());
				count += 1.0;
				if (o instanceof Date) {
					timeVal[0] += ((Date) o).getTime();
				}
				else if (o instanceof Calendar) {
					timeVal[0] += ((Calendar) o).getTime().getTime();
				}
			}
		}
		if (count > 0) {
			timeVal[0] = timeVal[0] / count;
		}
		return timeVal;
	}

	@Override
	public String[] getDimensionNames() {
		return TIME_NAME;
	}

}
