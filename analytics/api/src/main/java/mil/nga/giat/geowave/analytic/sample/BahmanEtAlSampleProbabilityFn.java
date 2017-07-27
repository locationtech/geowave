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
package mil.nga.giat.geowave.analytic.sample;

/**
 * l * d^2(y,C)/phi_x(C) y is some point, C is a set of centroids and l is an
 * oversampling factor. As documented in section 3.3 in
 * 
 * Bahmani, Kumar, Moseley, Vassilvitskii and Vattani. Scalable K-means++. VLDB
 * Endowment Vol. 5, No. 7. 2012.
 */

public class BahmanEtAlSampleProbabilityFn implements
		SampleProbabilityFn
{

	@Override
	public double getProbability(
			final double weight,
			final double normalizingConstant,
			final int sampleSize ) {
		return (((double) sampleSize) * weight) / normalizingConstant;
	}

	@Override
	public boolean requiresConstant() {
		return true;
	}

}
