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
package mil.nga.giat.geowave.format.stanag4676;

import java.io.Serializable;
import java.util.Comparator;

public class ComparatorStanag4676EventWritable implements
		Comparator<Stanag4676EventWritable>,
		Serializable
{

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(
			final Stanag4676EventWritable obj1,
			final Stanag4676EventWritable obj2 ) {
		return obj1.TimeStamp.compareTo(obj2.TimeStamp);
	}
}
