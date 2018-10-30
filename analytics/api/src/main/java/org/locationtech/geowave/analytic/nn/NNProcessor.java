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
package org.locationtech.geowave.analytic.nn;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.locationtech.geowave.analytic.nn.NeighborList.InferType;
import org.locationtech.geowave.analytic.partitioner.Partitioner;
import org.locationtech.geowave.analytic.partitioner.Partitioner.PartitionData;
import org.locationtech.geowave.analytic.partitioner.Partitioner.PartitionDataCallback;
import org.locationtech.geowave.core.index.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class is designed to support secondary partitioning.
 * 
 * (1) Partition added data using a partitioner.
 * 
 * (2) Process data, perform the O(N^2) (e.g. ~ n^2/2) comparisons within those
 * partitions.
 * 
 * Custom plug-ins include (1) A factory for the neighbor list to track those
 * pairings of data whose distance feel under the provided minimum. (2) A
 * complete notification callback callback for each primary data.
 * 
 * The loop algorithms is For each primary compare to all remaining primary and
 * all secondary data items
 * 
 * A powerful performance enhancing tool is the inference mechanism associated
 * with the neighborhood lists. A list can have intelligence to decide that a
 * particular neighbor can be inferred and, therefore, can be removed from the
 * set of primaries to be inspected. This has no effect on secondaries.
 * 
 * The processor can be called multiple times, as the 'process' algorithm does
 * not alter its internal state. The notification callback can be used to alter
 * the internal state (e.g. calling 'add' or 'remove' methods). Caution should
 * used to alter internal state within the neighbor list.
 * 
 * 
 * 
 * @param <PARTITION_VALUE>
 * @param <STORE_VALUE>
 * 
 * @See Partitioner
 * @See Partitioner.PartitionData
 */
public class NNProcessor<PARTITION_VALUE, STORE_VALUE>
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(NNProcessor.class);

	final Map<PartitionData, PartitionData> uniqueSetOfPartitions = new HashMap<PartitionData, PartitionData>();
	final Map<PartitionData, Set<ByteArray>> partitionsToIds = new HashMap<PartitionData, Set<ByteArray>>();
	final Map<ByteArray, Set<PartitionData>> idsToPartition = new HashMap<ByteArray, Set<PartitionData>>();
	final Map<ByteArray, STORE_VALUE> primaries = new HashMap<ByteArray, STORE_VALUE>();
	final Map<ByteArray, STORE_VALUE> others = new HashMap<ByteArray, STORE_VALUE>();

	protected final Partitioner<Object> partitioner;
	protected final TypeConverter<STORE_VALUE> typeConverter;

	protected final DistanceProfileGenerateFn<?, STORE_VALUE> distanceProfileFn;
	protected final double maxDistance;
	protected final PartitionData parentPartition;
	private int upperBoundPerPartition = DEFAULT_UPPER_BOUND_PARTIION_SIZE;

	public static final int DEFAULT_UPPER_BOUND_PARTIION_SIZE = 75000;

	/**
	 * Run State
	 */
	protected ByteArray startingPoint;
	protected NeighborIndex<STORE_VALUE> index;

	public NNProcessor(
			Partitioner<Object> partitioner,
			TypeConverter<STORE_VALUE> typeConverter,
			DistanceProfileGenerateFn<?, STORE_VALUE> distanceProfileFn,
			double maxDistance,
			PartitionData parentPartition ) {
		super();
		this.partitioner = partitioner;
		this.typeConverter = typeConverter;
		this.distanceProfileFn = distanceProfileFn;
		this.maxDistance = maxDistance;
		this.parentPartition = parentPartition;
	}

	private PartitionData add(
			final PartitionData pd,
			final ByteArray itemId ) {
		PartitionData singleton = uniqueSetOfPartitions.get(pd);
		if (singleton == null) {
			uniqueSetOfPartitions.put(
					pd,
					pd);
			singleton = pd;
		}

		Set<ByteArray> idsSet = partitionsToIds.get(singleton);
		if (idsSet == null) {
			idsSet = new HashSet<ByteArray>();
			partitionsToIds.put(
					singleton,
					idsSet);
		}
		if (idsSet.size() > upperBoundPerPartition) {
			return null;
		}
		if (idsSet.size() == upperBoundPerPartition) {
			LOGGER.warn("At upper bound on partition.  Increase the bounds or condense the data.");
		}
		idsSet.add(itemId);

		Set<PartitionData> partitionSet = idsToPartition.get(itemId);
		if (partitionSet == null) {
			partitionSet = new HashSet<PartitionData>();
			idsToPartition.put(
					itemId,
					partitionSet);
		}
		partitionSet.add(singleton);

		return singleton;
	}

	public void remove(
			final ByteArray id ) {

		final Set<PartitionData> partitionSet = idsToPartition.remove(id);
		if (partitionSet != null) {
			for (PartitionData pd : partitionSet) {
				final Set<ByteArray> idSet = partitionsToIds.get(pd);
				if (idSet != null) idSet.remove(id);
			}
		}
		primaries.remove(id);
		others.remove(id);
		if (index != null) {
			index.empty(id);
		}
	}

	public void add(
			final ByteArray id,
			final boolean isPrimary,
			final PARTITION_VALUE partitionValue )
			throws IOException {

		final STORE_VALUE storeValue = this.typeConverter.convert(
				id,
				partitionValue);

		try {
			partitioner.partition(
					partitionValue,
					new PartitionDataCallback() {

						@Override
						public void partitionWith(
								final PartitionData partitionData )
								throws Exception {
							PartitionData singleton = add(
									partitionData,
									id);
							if (singleton != null) {
								singleton.setPrimary(partitionData.isPrimary() || singleton.isPrimary());
								if (isPrimary)
									primaries.put(
											id,
											storeValue);
								else
									others.put(
											id,
											storeValue);
							}
						}
					});

		}
		catch (Exception e) {
			throw new IOException(
					e);
		}

		if (isPrimary) {
			if (startingPoint == null) startingPoint = id;
		}
	}

	public interface CompleteNotifier<STORE_VALUE>
	{
		public void complete(
				ByteArray id,
				STORE_VALUE value,
				NeighborList<STORE_VALUE> list )
				throws IOException,
				InterruptedException;
	}

	public int size() {
		return primaries.size() + others.size();
	}

	/**
	 * 
	 * @param size
	 *            the minimum size of a partition to be processed
	 * @return true if all partitions are emptt
	 */
	public boolean trimSmallPartitions(
			int size ) {
		Iterator<Map.Entry<PartitionData, Set<ByteArray>>> it = partitionsToIds.entrySet().iterator();
		while (it.hasNext()) {
			final Map.Entry<PartitionData, Set<ByteArray>> entry = it.next();
			if (entry.getValue().size() < size) {
				for (ByteArray id : entry.getValue()) {
					final Set<PartitionData> partitionsForId = idsToPartition.get(id);
					partitionsForId.remove(entry.getKey());
					if (partitionsForId.isEmpty()) {
						this.primaries.remove(id);
						this.others.remove(id);
					}
				}
				it.remove();
			}
		}
		return partitionsToIds.isEmpty();
	}

	public void process(
			NeighborListFactory<STORE_VALUE> listFactory,
			final CompleteNotifier<STORE_VALUE> notification )
			throws IOException,
			InterruptedException {

		LOGGER.info("Processing " + parentPartition.toString() + " with primary = " + primaries.size()
				+ " and other = " + others.size());
		LOGGER.info("Processing " + parentPartition.toString() + " with sub-partitions = "
				+ uniqueSetOfPartitions.size());

		index = new NeighborIndex<STORE_VALUE>(
				listFactory);

		double farthestDistance = 0;
		ByteArray farthestNeighbor = null;
		ByteArray nextStart = startingPoint;
		final Set<ByteArray> inspectionSet = new HashSet<ByteArray>();
		inspectionSet.addAll(primaries.keySet());

		if (inspectionSet.size() > 0 && nextStart == null) {
			nextStart = inspectionSet.iterator().next();
		}

		while (nextStart != null) {
			inspectionSet.remove(nextStart);
			farthestDistance = 0;
			final Set<PartitionData> partition = idsToPartition.get(nextStart);
			final STORE_VALUE primary = primaries.get(nextStart);
			final ByteArray primaryId = nextStart;
			nextStart = null;
			farthestNeighbor = null;
			if (LOGGER.isTraceEnabled()) LOGGER.trace("processing " + primaryId);
			if (primary == null) {
				if (inspectionSet.size() > 0) {
					nextStart = inspectionSet.iterator().next();
				}
				continue;
			}
			final NeighborList<STORE_VALUE> primaryList = index.init(
					primaryId,
					primary);

			for (PartitionData pd : partition) {
				for (ByteArray neighborId : partitionsToIds.get(pd)) {
					if (neighborId.equals(primaryId)) continue;
					boolean isAPrimary = true;
					STORE_VALUE neighbor = primaries.get(neighborId);
					if (neighbor == null) {
						neighbor = others.get(neighborId);
						isAPrimary = false;
					}
					else // prior processed primary
					if (!inspectionSet.contains(neighborId)) continue;

					if (neighbor == null) continue;
					final InferType inferResult = primaryList.infer(
							neighborId,
							neighbor);
					if (inferResult == InferType.NONE) {
						final DistanceProfile<?> distanceProfile = distanceProfileFn.computeProfile(
								primary,
								neighbor);
						final double distance = distanceProfile.getDistance();
						if (distance <= maxDistance) {
							index.add(
									distanceProfile,
									primaryId,
									primary,
									neighborId,
									neighbor,
									isAPrimary);
							if (LOGGER.isTraceEnabled()) LOGGER.trace("Neighbor " + neighborId);
						}
						if (distance > farthestDistance && inspectionSet.contains(neighborId)) {
							farthestDistance = distance;
							farthestNeighbor = neighborId;
						}
					}
					else if (inferResult == InferType.REMOVE) {
						inspectionSet.remove(neighborId);
					}
				}
			}
			notification.complete(
					primaryId,
					primary,
					primaryList);
			index.empty(primaryId);
			if (farthestNeighbor == null && inspectionSet.size() > 0) {
				nextStart = inspectionSet.iterator().next();
			}
			else {
				nextStart = farthestNeighbor;
			}
		}

	}

	public int getUpperBoundPerPartition() {
		return upperBoundPerPartition;
	}

	public void setUpperBoundPerPartition(
			int upperBoundPerPartition ) {
		this.upperBoundPerPartition = upperBoundPerPartition;
	}
}
