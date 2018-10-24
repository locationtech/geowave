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
package org.locationtech.geowave.core.store.entities;

import java.nio.ByteBuffer;

public interface GeoWaveKey
{
	public byte[] getDataId();

	public short getAdapterId();

	public byte[] getSortKey();
	
	public byte[] getPartitionKey();

	public int getNumberOfDuplicates();
	
	public static byte[] getCompositeId(GeoWaveKey key){
		final ByteBuffer buffer = ByteBuffer.allocate(
				key.getPartitionKey().length + key.getSortKey().length + key.getDataId().length + 6);
		buffer.put(
				key.getPartitionKey());
		buffer.put(
				key.getSortKey());
		buffer.putShort(
				key.getAdapterId());
		buffer.put(
				key.getDataId());
		buffer.putShort((short)
				key.getDataId().length);
		buffer.putShort(
				(short)key.getNumberOfDuplicates());
		buffer.rewind();
		return buffer.array();
	}
}
