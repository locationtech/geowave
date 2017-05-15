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
package mil.nga.giat.geowave.datastore.accumulo.encoding;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadData;

public class AccumuloUnreadDataList implements
		FlattenedUnreadData
{
	private final List<FlattenedUnreadData> unreadData;
	private List<FlattenedFieldInfo> cachedRead;

	public AccumuloUnreadDataList(
			final List<FlattenedUnreadData> unreadData ) {
		this.unreadData = unreadData;
	}

	@Override
	public List<FlattenedFieldInfo> finishRead() {
		if (cachedRead == null) {
			cachedRead = new ArrayList<>();
			for (final FlattenedUnreadData d : unreadData) {
				cachedRead.addAll(d.finishRead());
			}
		}
		return cachedRead;
	}
}
