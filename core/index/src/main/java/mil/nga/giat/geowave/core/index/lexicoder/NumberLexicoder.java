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

/**
 * A lexicoder for a number type. Converts back and forth between a number and a
 * byte array. A lexicographical sorting of the byte arrays will yield the
 * natural order of the numbers that they represent.
 * 
 * @param <T>
 *            a number type
 */
public interface NumberLexicoder<T extends Number>
{
	/**
	 * Get a byte[] that represents the number value.
	 * 
	 * @param value
	 *            a number
	 * @return the byte array representing the number
	 */
	public byte[] toByteArray(
			T value );

	/**
	 * Get the value of a byte array
	 * 
	 * @param bytes
	 *            a byte array representing a number
	 * @return the number
	 */
	public T fromByteArray(
			byte[] bytes );

	/**
	 * Get the minimum value of the range of numbers that this lexicoder can
	 * encode and decode (i.e. the number represented by all 0 bits).
	 * 
	 * @return the minimum value in the lexicoder's range
	 */
	public T getMinimumValue();

	/**
	 * Get the maximum value of the range of numbers that this lexicoder can
	 * encode and decode (i.e. the number represented by all 1 bits).
	 * 
	 * @return the maximum value in the lexicoder's range
	 */
	public T getMaximumValue();
}
