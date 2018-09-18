/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.mapreduce;

import org.apache.hadoop.io.Writable;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * This is an interface that extends data adapter to allow map reduce jobs to
 * easily convert hadoop writable objects to and from the geowave native
 * representation of the objects. This allow for generally applicable map reduce
 * jobs to be written using base classes for the mapper that can handle
 * translations.
 * 
 * @param <T>
 *            the native type
 * @param <W>
 *            the writable type
 */
public interface HadoopDataAdapter<T, W extends Writable> extends
		DataTypeAdapter<T>
{
	public HadoopWritableSerializer<T, W> createWritableSerializer();
}
