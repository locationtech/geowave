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
package mil.nga.giat.geowave.core.index.lexicoder;

import com.google.common.primitives.SignedBytes;

public class DoubleLexicoderTest extends
		AbstractLexicoderTest<Double>
{
	public DoubleLexicoderTest() {
		super(
				Lexicoders.DOUBLE,
				Double.MIN_VALUE,
				Double.MAX_VALUE,
				new Double[] {
					-10d,
					Double.MIN_VALUE,
					11d,
					-14.2,
					14.2,
					-100.002,
					100.002,
					-11d,
					Double.MAX_VALUE,
					0d
				},
				SignedBytes.lexicographicalComparator());
	}
}
