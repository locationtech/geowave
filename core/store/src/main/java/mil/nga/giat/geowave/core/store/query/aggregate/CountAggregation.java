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
package mil.nga.giat.geowave.core.store.query.aggregate;

import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;

public class CountAggregation implements
		CommonIndexAggregation<Persistable, CountResult>
{
	private long count = Long.MIN_VALUE;

	public CountAggregation() {}

	public boolean isSet() {
		return count != Long.MIN_VALUE;
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"count[count=").append(
				count);
		buffer.append("]");
		return buffer.toString();
	}

	@Override
	public void aggregate(
			final CommonIndexedPersistenceEncoding entry ) {
		if (!isSet()) {
			count = 0;
		}

		count += 1;
	}

	@Override
	public Persistable getParameters() {
		return null;
	}

	@Override
	public CountResult getResult() {
		if (!isSet()) {
			return null;
		}

		return new CountResult(
				count);
	}

	@Override
	public void setParameters(
			final Persistable parameters ) {}

	@Override
	public void clearResult() {
		count = 0;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}
}
