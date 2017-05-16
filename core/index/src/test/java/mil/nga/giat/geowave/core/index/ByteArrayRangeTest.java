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
package mil.nga.giat.geowave.core.index;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import mil.nga.giat.geowave.core.index.ByteArrayRange.MergeOperation;

import org.junit.Test;

public class ByteArrayRangeTest
{

	@Test
	public void testUnion() {
		ByteArrayRange bar1 = new ByteArrayRange(
				new ByteArrayId(
						"232"),
				new ByteArrayId(
						"332"));
		ByteArrayRange bar2 = new ByteArrayRange(
				new ByteArrayId(
						"282"),
				new ByteArrayId(
						"300"));
		ByteArrayRange bar3 = new ByteArrayRange(
				new ByteArrayId(
						"272"),
				new ByteArrayId(
						"340"));
		ByteArrayRange bar4 = new ByteArrayRange(
				new ByteArrayId(
						"392"),
				new ByteArrayId(
						"410"));

		List<ByteArrayRange> l1 = new ArrayList<ByteArrayRange>(
				Arrays.asList(
						bar4,
						bar3,
						bar1,
						bar2));
		l1 = ByteArrayRange.mergeIntersections(
				l1,
				MergeOperation.UNION);

		List<ByteArrayRange> l2 = new ArrayList<ByteArrayRange>(
				Arrays.asList(
						bar1,
						bar4,
						bar2,
						bar3));
		l2 = ByteArrayRange.mergeIntersections(
				l2,
				MergeOperation.UNION);

		assertEquals(
				2,
				l1.size());

		assertEquals(
				l1,
				l2);

		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								"232"),
						new ByteArrayId(
								"340")),
				l1.get(0));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								"392"),
						new ByteArrayId(
								"410")),
				l1.get(1));

	}

	@Test
	public void testIntersection() {
		ByteArrayRange bar1 = new ByteArrayRange(
				new ByteArrayId(
						"232"),
				new ByteArrayId(
						"332"));
		ByteArrayRange bar2 = new ByteArrayRange(
				new ByteArrayId(
						"282"),
				new ByteArrayId(
						"300"));
		ByteArrayRange bar3 = new ByteArrayRange(
				new ByteArrayId(
						"272"),
				new ByteArrayId(
						"340"));
		ByteArrayRange bar4 = new ByteArrayRange(
				new ByteArrayId(
						"392"),
				new ByteArrayId(
						"410"));

		List<ByteArrayRange> l1 = new ArrayList<ByteArrayRange>(
				Arrays.asList(
						bar4,
						bar3,
						bar1,
						bar2));
		l1 = ByteArrayRange.mergeIntersections(
				l1,
				MergeOperation.INTERSECTION);

		List<ByteArrayRange> l2 = new ArrayList<ByteArrayRange>(
				Arrays.asList(
						bar1,
						bar4,
						bar2,
						bar3));
		l2 = ByteArrayRange.mergeIntersections(
				l2,
				MergeOperation.INTERSECTION);

		assertEquals(
				2,
				l1.size());

		assertEquals(
				l1,
				l2);

		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								"282"),
						new ByteArrayId(
								"300")),
				l1.get(0));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								"392"),
						new ByteArrayId(
								"410")),
				l1.get(1));

	}

	final Random random = new Random();

	public String increment(
			String id ) {
		int v = (int) (Math.abs(random.nextDouble()) * 10000);
		StringBuffer buf = new StringBuffer();
		int pos = id.length() - 1;
		int r = 0;
		while (v > 0) {
			int m = (v - ((v >> 8) << 8));
			int c = id.charAt(pos);
			int n = c + m + r;
			buf.append((char) (n % 255));
			r = n / 255;
			v >>= 8;
			pos--;
		}
		while (pos >= 0) {
			buf.append(id.charAt(pos--));
		}
		return buf.reverse().toString();
	}

	@Test
	public void bigTest() {
		List<ByteArrayRange> l2 = new ArrayList<ByteArrayRange>();
		for (int i = 0; i < 3000; i++) {
			String seed = UUID.randomUUID().toString();
			for (int j = 0; j < 500; j++) {
				l2.add(new ByteArrayRange(
						new ByteArrayId(
								seed),
						new ByteArrayId(
								increment(seed))));
				seed = increment(seed);
			}
		}

		ByteArrayRange.mergeIntersections(
				l2,
				MergeOperation.INTERSECTION);

	}
}
