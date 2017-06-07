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

import java.util.Random;

public class RandomProbabilitySampleFn implements
		SampleProbabilityFn
{
	final Random random = new Random();

	@Override
	public double getProbability(
			final double weight,
			final double normalizingConstant,
			final int sampleSize ) {
		// HP Fortify "Insecure Randomness" false positive
		// This random number is not used for any purpose
		// related to security or cryptography
		return Math.log(random.nextDouble()) / (weight / normalizingConstant);
	}

	@Override
	public boolean requiresConstant() {
		return false;
	}

}
