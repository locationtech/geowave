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
package mil.nga.giat.geowave.core.store.server;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.server.ServerOpConfig.OptionProvider;

public class RowMergingAdapterOptionProvider implements
		OptionProvider
{
	public static final String ROW_TRANSFORM_KEY = "ROW_TRANSFORM";
	public static final String ROW_MERGING_ADAPTER_CACHE_ID = "ROW_MERGING_ADAPTER";
	public static final String ADAPTER_IDS_OPTION = "adapters";

	private final RowMergingDataAdapter<?, ?> adapter;
	private final short internalAdapterId;

	public RowMergingAdapterOptionProvider(
			final short internalAdapterId,
			final RowMergingDataAdapter<?, ?> adapter ) {
		this.internalAdapterId = internalAdapterId;
		this.adapter = adapter;
	}

	@Override
	public Map<String, String> getOptions(
			final Map<String, String> existingOptions ) {
		final Map<String, String> newOptions = adapter.getOptions(
				internalAdapterId,
				existingOptions);

		String nextAdapterIdsValue = ByteArrayUtils.shortToString(internalAdapterId);

		if ((existingOptions != null) && existingOptions.containsKey(ADAPTER_IDS_OPTION)) {
			final String existingAdapterIds = existingOptions.get(ADAPTER_IDS_OPTION);
			final Set<String> nextAdapters = new HashSet<String>();
			for (final String id : nextAdapterIdsValue.split(",")) {
				nextAdapters.add(id);
			}
			final StringBuffer str = new StringBuffer(
					nextAdapterIdsValue);
			for (final String id : existingAdapterIds.split(",")) {
				if (!nextAdapters.contains(id)) {
					str.append(",");
					str.append(id);
				}
			}
			nextAdapterIdsValue = str.toString();
		}
		newOptions.put(
				ADAPTER_IDS_OPTION,
				nextAdapterIdsValue);
		newOptions.put(
				ROW_TRANSFORM_KEY,
				ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(adapter.getTransform())));
		return newOptions;
	}
}
