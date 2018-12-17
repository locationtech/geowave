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

import org.locationtech.geowave.core.index.VarintUtils;

public interface GeoWaveKey
{
	public byte[] getDataId();

	public short getAdapterId();

	public byte[] getSortKey();
	
	public byte[] getPartitionKey();

	public int getNumberOfDuplicates();
	
	public static byte[] getCompositeId(GeoWaveKey key){
		final ByteBuffer buffer = ByteBuffer.allocate(
				key.getPartitionKey().length + key.getSortKey().length + key.getDataId().length +
				VarintUtils.unsignedIntByteLength(key.getAdapterId() & 0xFFFF) +
				VarintUtils.unsignedIntByteLength(key.getDataId().length) +
				VarintUtils.unsignedIntByteLength(key.getNumberOfDuplicates()));
		buffer.put(
				key.getPartitionKey());
		buffer.put(
				key.getSortKey());
		VarintUtils.writeUnsignedIntReversed(key.getAdapterId() & 0xFFFF, buffer);
		buffer.put(
				key.getDataId());
		VarintUtils.writeUnsignedIntReversed(key.getDataId().length, buffer);
		VarintUtils.writeUnsignedIntReversed(key.getNumberOfDuplicates(), buffer);
		buffer.rewind();
		return buffer.array();
	}
}
