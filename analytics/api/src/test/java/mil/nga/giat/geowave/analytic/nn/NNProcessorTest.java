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
package mil.nga.giat.geowave.analytic.nn;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.nn.NNProcessor.CompleteNotifier;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionData;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Before;
import org.junit.Test;

public class NNProcessorTest
{
	static Map<Integer, List<Integer>> expectedResults = new HashMap<Integer, List<Integer>>();

	@Before
	public void setupResults() {
		expectedResults.put(
				new Integer(
						293),
				Arrays.asList(new Integer(
						233)));
		expectedResults.put(
				new Integer(
						233),
				Arrays.asList(new Integer(
						293)));
		expectedResults.put(
				new Integer(
						735),
				Arrays.asList(new Integer(
						833)));
		expectedResults.put(
				new Integer(
						833),
				Arrays.asList(new Integer(
						735)));
		expectedResults.put(
				new Integer(
						1833),
				Arrays.asList(new Integer(
						2033)));
		expectedResults.put(
				new Integer(
						2033),
				Arrays.asList(new Integer(
						1833)));
		expectedResults.put(
				new Integer(
						1033),
				Collections.<Integer> emptyList());
		expectedResults.put(
				new Integer(
						533),
				Collections.<Integer> emptyList());
	}

	NNProcessor<Integer, Integer> buildProcessor() {
		return new NNProcessor<Integer, Integer>(
				new Partitioner<Object>() {

					@Override
					public void initialize(
							final JobContext context,
							final Class<?> scope )
							throws IOException {}

					@Override
					public List<mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionData> getCubeIdentifiers(
							final Object entry ) {
						return Collections.singletonList(new PartitionData(
								NNProcessorTest.partition((Integer) entry),
								true));
					}

					@Override
					public void partition(
							final Object entry,
							final mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionDataCallback callback )
							throws Exception {
						for (final PartitionData pd : getCubeIdentifiers(entry)) {
							callback.partitionWith(pd);
						}

					}

					@Override
					public Collection<ParameterEnum<?>> getParameters() {
						return Collections.emptyList();
					}

					@Override
					public void setup(
							final PropertyManagement runTimeProperties,
							final Class<?> scope,
							final Configuration configuration ) {}
				},
				new TypeConverter<Integer>() {
					@Override
					public Integer convert(
							final ByteArrayId id,
							final Object o ) {
						return (Integer) o;
					}

				},
				new DistanceProfileGenerateFn<Integer, Integer>() {

					@Override
					public DistanceProfile<Integer> computeProfile(
							final Integer item1,
							final Integer item2 ) {
						return new DistanceProfile<Integer>(
								Math.abs(item1.doubleValue() - item2.doubleValue()),
								item1);
					}
				},
				200,
				new PartitionData(
						new ByteArrayId(
								"123"),
						true));
	}

	@Test
	public void testNormalOp()
			throws IOException,
			InterruptedException {

		runProcess(
				buildProcessor(),
				new CompleteNotifier<Integer>() {

					@Override
					public void complete(
							final ByteArrayId id,
							final Integer value,
							final NeighborList<Integer> list )
							throws IOException,
							InterruptedException {
						final Iterator<Entry<ByteArrayId, Integer>> it = list.iterator();
						final List<Integer> expectedResultSet = new ArrayList<Integer>(
								expectedResults.get(value));
						assertNotNull(expectedResultSet);
						while (it.hasNext()) {
							final Integer result = it.next().getValue();
							assertTrue(
									"" + value + " with " + result,
									expectedResultSet.remove(result));
						}
						assertTrue(expectedResultSet.isEmpty());
					}

				});
	}

	@Test
	public void testRemoveOp()
			throws IOException,
			InterruptedException {
		final NNProcessor<Integer, Integer> processor = buildProcessor();
		runProcess(
				processor,
				new CompleteNotifier<Integer>() {

					@Override
					public void complete(
							final ByteArrayId id,
							final Integer value,
							final NeighborList<Integer> list )
							throws IOException,
							InterruptedException {
						processor.remove(id);
					}
				});
	}

	@Test
	public void testTrimOp()
			throws IOException,
			InterruptedException {
		final NNProcessor<Integer, Integer> processor = buildProcessor();
		addToProcess(
				processor,
				293);
		addToProcess(
				processor,
				233);
		addToProcess(
				processor,
				533);
		addToProcess(
				processor,
				735);
		addToProcess(
				processor,
				833);
		addToProcess(
				processor,
				1033);
		addToProcess(
				processor,
				1833);
		addToProcess(
				processor,
				2033);
		processor.trimSmallPartitions(10);
		processor.process(
				new NeighborListFactory<Integer>() {

					@Override
					public NeighborList<Integer> buildNeighborList(
							final ByteArrayId cnterId,
							final Integer center ) {
						return new DefaultNeighborList<Integer>();
					}

				},
				new CompleteNotifier<Integer>() {

					@Override
					public void complete(
							final ByteArrayId id,
							final Integer value,
							final NeighborList<Integer> list )
							throws IOException,
							InterruptedException {
						fail("Should not get here");
					}
				});
	}

	private void runProcess(
			final NNProcessor<Integer, Integer> processor,
			final CompleteNotifier<Integer> notifier )
			throws IOException,
			InterruptedException {

		addToProcess(
				processor,
				293);
		addToProcess(
				processor,
				233);
		addToProcess(
				processor,
				533);
		addToProcess(
				processor,
				735);
		addToProcess(
				processor,
				833);
		addToProcess(
				processor,
				1033);
		addToProcess(
				processor,
				1833);
		addToProcess(
				processor,
				2033);

		processor.process(
				new NeighborListFactory<Integer>() {

					@Override
					public NeighborList<Integer> buildNeighborList(
							final ByteArrayId cnterId,
							final Integer center ) {
						return new DefaultNeighborList<Integer>();
					}

				},
				notifier);

	}

	private static ByteArrayId partition(
			final Integer v ) {
		return new ByteArrayId(
				Integer.toString((v.intValue() / 300)));
	}

	private void addToProcess(
			final NNProcessor<Integer, Integer> processor,
			final Integer v )
			throws IOException {
		processor.add(
				new ByteArrayId(
						v.toString()),
				true,
				v);
	}
}
