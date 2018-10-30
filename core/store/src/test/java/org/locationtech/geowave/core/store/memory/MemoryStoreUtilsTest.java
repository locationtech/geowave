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
package org.locationtech.geowave.core.store.memory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.adapter.AbstractDataAdapter;
import org.locationtech.geowave.core.store.adapter.MockComponents;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsProvider;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.memory.MemoryStoreUtils;

public class MemoryStoreUtilsTest
{
	@Test
	public void testVisibility() {
		assertTrue(MemoryStoreUtils.isAuthorized(
				"aaa&ccc".getBytes(),
				new String[] {
					"aaa",
					"bbb",
					"ccc"
				}));

		assertFalse(MemoryStoreUtils.isAuthorized(
				"aaa&ccc".getBytes(),
				new String[] {
					"aaa",
					"bbb"
				}));

		assertTrue(MemoryStoreUtils.isAuthorized(
				"aaa&(ccc|eee)".getBytes(),
				new String[] {
					"aaa",
					"eee",
					"xxx"
				}));

		assertTrue(MemoryStoreUtils.isAuthorized(
				"aaa|(ccc&eee)".getBytes(),
				new String[] {
					"bbb",
					"eee",
					"ccc"
				}));

		assertFalse(MemoryStoreUtils.isAuthorized(
				"aaa|(ccc&eee)".getBytes(),
				new String[] {
					"bbb",
					"dddd",
					"ccc"
				}));

		assertTrue(MemoryStoreUtils.isAuthorized(
				"aaa|(ccc&eee)".getBytes(),
				new String[] {
					"aaa",
					"dddd",
					"ccc"
				}));

		assertTrue(MemoryStoreUtils.isAuthorized(
				"aaa".getBytes(),
				new String[] {
					"aaa",
					"dddd",
					"ccc"
				}));

		assertFalse(MemoryStoreUtils.isAuthorized(
				"xxx".getBytes(),
				new String[] {
					"aaa",
					"dddd",
					"ccc"
				}));

	}
}
