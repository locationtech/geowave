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
package org.locationtech.geowave.core.index;

import org.locationtech.geowave.core.index.CompoundIndexStrategy.CompoundIndexMetaDataWrapper;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray.ArrayOfArrays;
import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.UnboundedDimensionDefinition;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.core.index.sfc.SFCDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.BinnedNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.index.sfc.data.NumericValue;
import org.locationtech.geowave.core.index.sfc.hilbert.HilbertSFC;
import org.locationtech.geowave.core.index.sfc.tiered.SingleTierSubStrategy;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy.TierIndexMetaData;
import org.locationtech.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy;
import org.locationtech.geowave.core.index.sfc.xz.XZOrderSFC;
import org.locationtech.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy.XZHierarchicalIndexMetaData;
import org.locationtech.geowave.core.index.sfc.zorder.ZOrderSFC;
import org.locationtech.geowave.core.index.simple.HashKeyIndexStrategy;
import org.locationtech.geowave.core.index.simple.RoundRobinKeyIndexStrategy;
import org.locationtech.geowave.core.index.simple.SimpleIntegerIndexStrategy;
import org.locationtech.geowave.core.index.simple.SimpleLongIndexStrategy;
import org.locationtech.geowave.core.index.simple.SimpleShortIndexStrategy;

public class IndexPersistableRegistry implements
		PersistableRegistrySpi
{

	@Override
	public PersistableIdAndConstructor[] getSupportedPersistables() {
		return new PersistableIdAndConstructor[] {
			new PersistableIdAndConstructor(
					(short) 100,
					CompoundIndexMetaDataWrapper::new),
			new PersistableIdAndConstructor(
					(short) 101,
					TierIndexMetaData::new),
			new PersistableIdAndConstructor(
					(short) 102,
					CompoundIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 103,
					CoordinateRange::new),
			new PersistableIdAndConstructor(
					(short) 104,
					MultiDimensionalCoordinateRanges::new),
			new PersistableIdAndConstructor(
					(short) 105,
					ArrayOfArrays::new),
			new PersistableIdAndConstructor(
					(short) 106,
					MultiDimensionalCoordinateRangesArray::new),
			new PersistableIdAndConstructor(
					(short) 107,
					NullNumericIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 108,
					NumericIndexStrategyWrapper::new),
			new PersistableIdAndConstructor(
					(short) 109,
					BasicDimensionDefinition::new),
			new PersistableIdAndConstructor(
					(short) 110,
					UnboundedDimensionDefinition::new),
			new PersistableIdAndConstructor(
					(short) 111,
					SFCDimensionDefinition::new),
			new PersistableIdAndConstructor(
					(short) 112,
					BasicNumericDataset::new),
			new PersistableIdAndConstructor(
					(short) 113,
					BinnedNumericDataset::new),
			new PersistableIdAndConstructor(
					(short) 114,
					NumericRange::new),
			new PersistableIdAndConstructor(
					(short) 115,
					NumericValue::new),
			new PersistableIdAndConstructor(
					(short) 116,
					HilbertSFC::new),
			new PersistableIdAndConstructor(
					(short) 117,
					SingleTierSubStrategy::new),
			new PersistableIdAndConstructor(
					(short) 118,
					TieredSFCIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 119,
					XZHierarchicalIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 120,
					XZOrderSFC::new),
			new PersistableIdAndConstructor(
					(short) 121,
					ZOrderSFC::new),
			new PersistableIdAndConstructor(
					(short) 122,
					HashKeyIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 123,
					RoundRobinKeyIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 124,
					SimpleIntegerIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 125,
					SimpleLongIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 126,
					SimpleShortIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 127,
					XZHierarchicalIndexMetaData::new),
			new PersistableIdAndConstructor(
					(short) 128,
					InsertionIds::new),
			new PersistableIdAndConstructor(
					(short) 129,
					PartitionIndexStrategyWrapper::new),
			new PersistableIdAndConstructor(
					(short) 130,
					SinglePartitionInsertionIds::new),
		};
	}
}
