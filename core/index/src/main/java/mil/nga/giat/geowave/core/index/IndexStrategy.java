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

import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.persist.Persistable;

/**
 * Interface which defines an index strategy.
 * 
 */
public interface IndexStrategy<QueryRangeType extends QueryConstraints, EntryRangeType> extends
		Persistable
{
	/**
	 * Returns a list of query ranges for an specified numeric range.
	 * 
	 * @param indexedRange
	 *            defines the numeric range for the query
	 * @return a List of query ranges
	 */
	public List<ByteArrayRange> getQueryRanges(
			QueryRangeType indexedRange,
			IndexMetaData... hints );

	/**
	 * Returns a list of query ranges for an specified numeric range.
	 * 
	 * @param indexedRange
	 *            defines the numeric range for the query
	 * @param maxRangeDecomposition
	 *            the maximum number of ranges provided by a single query
	 *            decomposition, this is a best attempt and not a guarantee
	 * @return a List of query ranges
	 */
	public List<ByteArrayRange> getQueryRanges(
			QueryRangeType indexedRange,
			int maxEstimatedRangeDecomposition,
			IndexMetaData... hints );

	/**
	 * Returns a list of id's for insertion. The index strategy will use a
	 * reasonable default for the maximum duplication of insertion IDs
	 * 
	 * @param indexedData
	 *            defines the numeric data to be indexed
	 * @return a List of insertion ID's
	 */
	public List<ByteArrayId> getInsertionIds(
			EntryRangeType indexedData );

	/**
	 * Returns a list of id's for insertion.
	 * 
	 * @param indexedData
	 *            defines the numeric data to be indexed
	 * @param maxDuplicateInsertionIds
	 *            defines the maximum number of insertion IDs that can be used,
	 *            this is a best attempt and not a guarantee
	 * @return a List of insertion ID's
	 */
	public List<ByteArrayId> getInsertionIds(
			EntryRangeType indexedData,
			int maxEstimatedDuplicateIds );

	/**
	 * Returns the range that the given ID represents
	 * 
	 * @param insertionId
	 *            the insertion ID to determine a range for
	 * @return the range that the given insertion ID represents, inclusive on
	 *         the start and exclusive on the end for the range
	 */
	public EntryRangeType getRangeForId(
			ByteArrayId insertionId );

	/**
	 * 
	 * @return a unique ID associated with the index strategy
	 */
	public String getId();

	/***
	 * Get the set of natural splits for this index strategy. Not all index
	 * strategies need to have natural splits but in the cases that they exist,
	 * it can be used downstream to appropriately partition the data according
	 * to natural split points from the index strategy.
	 * 
	 * @return the set of natural splits
	 */
	public Set<ByteArrayId> getNaturalSplits();

	public List<IndexMetaData> createMetaData();

}
