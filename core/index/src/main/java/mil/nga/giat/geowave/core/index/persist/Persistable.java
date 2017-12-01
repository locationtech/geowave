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
package mil.nga.giat.geowave.core.index.persist;

/**
 * 
 * A simple interface for persisting objects, PersistenceUtils provides
 * convenience methods for serializing and de-serializing these objects
 * 
 */
public interface Persistable
{
	/**
	 * Convert fields and data within an object to binary form for transmission
	 * or storage.
	 * 
	 * @return an array of bytes representing a binary stream representation of
	 *         the object.
	 */
	public byte[] toBinary();

	/**
	 * Convert a stream of binary bytes to fields and data within an object.
	 * 
	 */
	public void fromBinary(
			byte[] bytes );

}
