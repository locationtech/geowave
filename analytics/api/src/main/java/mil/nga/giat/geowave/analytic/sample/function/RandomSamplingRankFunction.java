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
package mil.nga.giat.geowave.analytic.sample.function;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;

/**
 * Pick any object at random by assigning a random weight over a uniform
 * distribution.
 * 
 * @param <T>
 */
public class RandomSamplingRankFunction<T> implements
		SamplingRankFunction<T>
{
	private final Random random = new Random();

	@Override
	public void initialize(
			final JobContext context,
			final Class<?> scope,
			final Logger logger )
			throws IOException {}

	@Override
	public double rank(
			final int sampleSize,
			final T value ) {
		// HP Fortify "Insecure Randomness" false positive
		// This random number is not used for any purpose
		// related to security or cryptography
		return random.nextDouble();
	}
}
