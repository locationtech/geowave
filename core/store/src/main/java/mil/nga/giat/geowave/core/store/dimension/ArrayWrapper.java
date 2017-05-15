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
package mil.nga.giat.geowave.core.store.dimension;

import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

public class ArrayWrapper<T> implements
		CommonIndexValue
{
	private byte[] visibility;
	private final T[] array;

	public ArrayWrapper(
			final T[] array ) {
		this.array = array;
	}

	public ArrayWrapper(
			final T[] array,
			final byte[] visibility ) {
		this.visibility = visibility;
		this.array = array;
	}

	@Override
	public byte[] getVisibility() {
		return visibility;
	}

	@Override
	public void setVisibility(
			final byte[] visibility ) {
		this.visibility = visibility;
	}

	public T[] getArray() {
		return array;
	}

	@Override
	public boolean overlaps(
			NumericDimensionField[] field,
			NumericData[] rangeData ) {
		// TODO Auto-generated method stub
		return true;
	}

}
