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
package org.locationtech.geowave.core.store.query.filter;

import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

public class AdapterIdQueryFilter implements
		QueryFilter
{
	private Short adapterId;

	public AdapterIdQueryFilter() {}

	public AdapterIdQueryFilter(
			final short adapterId ) {
		this.adapterId = adapterId;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding persistenceEncoding ) {
		return (adapterId == null) || adapterId.equals((Short) persistenceEncoding.getInternalAdapterId());
	}

	@Override
	public byte[] toBinary() {
		if (adapterId == null) {
			return ByteArrayUtils.shortToByteArray((short) 0);
		}
		return ByteArrayUtils.shortToByteArray(adapterId);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if (ByteArrayUtils.byteArrayToShort(bytes) == 0) {
			adapterId = null;
		}
		else {
			adapterId = ByteArrayUtils.byteArrayToShort(bytes);
		}
	}
}
