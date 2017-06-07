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
package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * The Key-Value pair that will be emitted from a mapper and used by a reducer
 * in the IngestWithReducer flow.
 * 
 * @param <K>
 *            The type for the key to be emitted
 * @param <V>
 *            The type for the value to be emitted
 */
public class KeyValueData<K extends WritableComparable<?>, V extends Writable>
{
	private final K key;
	private final V value;

	public KeyValueData(
			final K key,
			final V value ) {
		this.key = key;
		this.value = value;
	}

	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

}
