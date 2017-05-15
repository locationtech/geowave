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
package mil.nga.giat.geowave.datastore.accumulo;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.datastore.accumulo.util.TransformerWriter;
import mil.nga.giat.geowave.datastore.accumulo.util.VisibilityTransformer;

public class TransformWriterTest
{
	private final MockInstance mockInstance = new MockInstance();
	private Connector mockConnector = null;
	private BasicAccumuloOperations operations;

	@Before
	public void setUp() {

		try {
			mockConnector = mockInstance.getConnector(
					"root",
					new PasswordToken(
							new byte[0]));

			operations = new BasicAccumuloOperations(
					mockConnector);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			e.printStackTrace();
		}

		operations.createTable(
				"test_table",
				true,
				true,
				null);
	}

	private void write(
			final Writer writer,
			final String id,
			final String cf,
			final String cq,
			final String vis,
			final String value ) {
		final Mutation m = new Mutation(
				new Text(
						id.getBytes(StringUtils.GEOWAVE_CHAR_SET)));
		m.put(
				new Text(
						cf),
				new Text(
						cq),
				new ColumnVisibility(
						vis),
				new Value(
						value.getBytes(StringUtils.GEOWAVE_CHAR_SET)));
		writer.write(m);
	}

	private class Expect
	{
		byte[] id;
		int count;

		public Expect(
				final byte[] id,
				final int count ) {
			super();
			this.id = id;
			this.count = count;
		}

	}

	private void check(
			final Iterator<Entry<Key, Value>> it,
			final Expect... expectations ) {
		final Map<ByteArrayId, Integer> result = new HashMap<ByteArrayId, Integer>();

		while (it.hasNext()) {
			final Entry<Key, Value> entry = it.next();
			final ByteArrayId rowID = new ByteArrayId(
					entry.getKey().getRow().getBytes());
			result.put(
					rowID,
					Integer.valueOf(1 + (result.containsKey(rowID) ? result.get(
							rowID).intValue() : 0)));
		}
		int expectedCount = 0;
		for (final Expect e : expectations) {
			final ByteArrayId rowID = new ByteArrayId(
					e.id);
			expectedCount += (e.count > 0 ? 1 : 0);
			assertEquals(
					new Text(
							e.id).toString(),
					e.count,
					(result.containsKey(rowID) ? result.get(
							rowID).intValue() : 0));
		}
		assertEquals(
				result.size(),
				expectedCount);
	}

	@Test
	public void test()
			throws TableNotFoundException,
			IOException {
		final Writer w = operations.createWriter("test_table");
		write(
				w,
				"1234",
				"cf1",
				"cq1",
				"a&b",
				"123");
		write(
				w,
				"1234",
				"cf2",
				"cq2",
				"a&b",
				"123");
		write(
				w,
				"1235",
				"cf1",
				"cq1",
				"a&b",
				"123");
		write(
				w,
				"1235",
				"cf2",
				"cq2",
				"a&b",
				"123");
		w.close();

		Scanner scanner = operations.createScanner(
				"test_table",
				"a",
				"b");
		check(
				scanner.iterator(),
				new Expect(
						"1234".getBytes(StringUtils.GEOWAVE_CHAR_SET),
						2),
				new Expect(
						"1235".getBytes(StringUtils.GEOWAVE_CHAR_SET),
						2));
		scanner.close();

		scanner = operations.createScanner(
				"test_table",
				"a",
				"c");
		check(
				scanner.iterator(),
				new Expect(
						"1234".getBytes(StringUtils.GEOWAVE_CHAR_SET),
						0),
				new Expect(
						"1235".getBytes(StringUtils.GEOWAVE_CHAR_SET),
						0));
		scanner.close();

		final VisibilityTransformer transformer = new VisibilityTransformer(
				"b",
				"c");
		scanner = operations.createScanner(
				"test_table",
				"a",
				"b",
				"c");
		final TransformerWriter tw = new TransformerWriter(
				scanner,
				"test_table",
				operations,
				transformer);
		tw.transform();
		scanner.close();

		scanner = operations.createScanner(
				"test_table",
				"a",
				"c");
		check(
				scanner.iterator(),
				new Expect(
						"1234".getBytes(),
						2),
				new Expect(
						"1235".getBytes(),
						2));
		scanner.close();

		scanner = operations.createScanner(
				"test_table",
				"a",
				"b");
		check(
				scanner.iterator(),
				new Expect(
						"1234".getBytes(),
						0),
				new Expect(
						"1235".getBytes(),
						0));
		scanner.close();

	}
}
