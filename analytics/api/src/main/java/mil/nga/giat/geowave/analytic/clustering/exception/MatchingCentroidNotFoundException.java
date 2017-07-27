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
package mil.nga.giat.geowave.analytic.clustering.exception;

public class MatchingCentroidNotFoundException extends
		Exception
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MatchingCentroidNotFoundException() {
		super();
	}

	public MatchingCentroidNotFoundException(
			final String arg0,
			final Throwable arg1,
			final boolean arg2,
			final boolean arg3 ) {
		super(
				arg0,
				arg1,
				arg2,
				arg3);
	}

	public MatchingCentroidNotFoundException(
			final String arg0,
			final Throwable arg1 ) {
		super(
				arg0,
				arg1);
	}

	public MatchingCentroidNotFoundException(
			final String arg0 ) {
		super(
				arg0);
	}

	public MatchingCentroidNotFoundException(
			final Throwable arg0 ) {
		super(
				arg0);
	}

}
