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
package mil.nga.giat.geowave.core.store.flatten;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FlattenedUnreadDataSingleRow implements
		FlattenedUnreadData
{
	private final ByteBuffer partiallyConsumedBuffer;
	private final int currentIndexInFieldPositions;
	private final List<Integer> fieldPositions;
	private List<FlattenedFieldInfo> cachedRead = null;

	public FlattenedUnreadDataSingleRow(
			final ByteBuffer partiallyConsumedBuffer,
			final int currentIndexInFieldPositions,
			final List<Integer> fieldPositions ) {
		this.partiallyConsumedBuffer = partiallyConsumedBuffer;
		this.currentIndexInFieldPositions = currentIndexInFieldPositions;
		this.fieldPositions = fieldPositions;
	}

	public List<FlattenedFieldInfo> finishRead() {
		if (cachedRead == null) {
			cachedRead = new ArrayList<>();
			for (int i = currentIndexInFieldPositions; i < fieldPositions.size(); i++) {
				final int fieldLength = partiallyConsumedBuffer.getInt();
				final byte[] fieldValueBytes = new byte[fieldLength];
				partiallyConsumedBuffer.get(fieldValueBytes);
				final Integer fieldPosition = fieldPositions.get(i);
				cachedRead.add(new FlattenedFieldInfo(
						fieldPosition,
						fieldValueBytes));
			}
		}
		return cachedRead;
	}
}
