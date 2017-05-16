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
package mil.nga.giat.geowave.analytic.nn;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * Convert object consumed by NN to a 'smaller' object pertinent to any subclass
 * algorithms
 * 
 * @param <TYPE>
 */
public interface TypeConverter<TYPE>
{
	public TYPE convert(
			ByteArrayId id,
			Object o );
}
