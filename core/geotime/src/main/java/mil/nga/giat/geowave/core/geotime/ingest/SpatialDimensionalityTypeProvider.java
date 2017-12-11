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
package mil.nga.giat.geowave.core.geotime.ingest;

import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.store.dimension.CustomCRSSpatialField;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.xz.XZHierarchicalIndexFactory;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions.BaseIndexBuilder;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeOptions;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeProviderSpi;

public class SpatialDimensionalityTypeProvider implements
		DimensionalityTypeProviderSpi
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SpatialDimensionalityTypeProvider.class);
	private final SpatialOptions options = new SpatialOptions();
	private static final String DEFAULT_SPATIAL_ID = "SPATIAL_IDX";
	private static final int LONGITUDE_BITS = 31;
	private static final int LATITUDE_BITS = 31;

	protected static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new LongitudeDefinition(),
		new LatitudeDefinition(
				true)
	// just use the same range for latitude to make square sfc values in
	// decimal degrees (EPSG:4326)
	};
	protected static final NumericDimensionField[] SPATIAL_FIELDS = new NumericDimensionField[] {
		new LongitudeField(),
		new LatitudeField(
				true)
	// just use the same range for latitude to make square sfc values in
	// decimal degrees (EPSG:4326)
	};
	protected static final NumericDimensionField[] SPATIAL_TEMPORAL_FIELDS = new NumericDimensionField[] {
		new LongitudeField(),
		new LatitudeField(
				true),
		new TimeField(
				Unit.YEAR)
	};

	public SpatialDimensionalityTypeProvider() {}

	@Override
	public String getDimensionalityTypeName() {
		return "spatial";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return "This dimensionality type matches all indices that only require Geometry.";
	}

	@Override
	public int getPriority() {
		// arbitrary - just higher than spatial temporal so that the default
		// will be spatial over spatial-temporal
		return 10;
	}

	@Override
	public DimensionalityTypeOptions getOptions() {
		return options;
	}

	@Override
	public PrimaryIndex createPrimaryIndex() {
		return internalCreatePrimaryIndex(options);
	}

	private static PrimaryIndex internalCreatePrimaryIndex(
			final SpatialOptions options ) {
		NumericDimensionDefinition[] dimensions;
		NumericDimensionField<?>[] fields;
		if (options.crs == null || options.crs.isEmpty() || options.crs.equalsIgnoreCase(GeometryUtils.DEFAULT_CRS_STR)){
		dimensions = SPATIAL_DIMENSIONS;
		fields = SPATIAL_FIELDS;
		}
		else{
			try {
				CoordinateReferenceSystem crs = CRS.decode(
						options.crs,
						true);
				CoordinateSystem cs = crs.getCoordinateSystem();
				dimensions = new NumericDimensionDefinition[cs.getDimension()];

				fields = new NumericDimensionField[dimensions.length];
				for (int d =0 ;d < dimensions.length;d++){
					CoordinateSystemAxis csa = cs.getAxis(d);
						dimensions[d]=new BasicDimensionDefinition(csa.getMinimumValue(), csa.getMaximumValue());
							fields[d] = new CustomCRSSpatialField((byte)d, dimensions[d]);
			}
			}
			catch (final FactoryException e) {
				LOGGER.error(
						"Unable to decode '"+options.crs+"' CRS",
						e);
				throw new RuntimeException(
						"Unable to initialize '"+options.crs+"' object",
						e);
			}
		}
		return new CustomIdIndex(
				XZHierarchicalIndexFactory.createFullIncrementalTieredStrategy(
						dimensions,
						new int[] {
								//TODO this is only valid for 2D coordinate systems, again consider the possibility of being flexible enough to handle n-dimensions
							LONGITUDE_BITS,
							LATITUDE_BITS
						},
						SFCType.HILBERT),
				new BasicIndexModel(//TODO append time to fields if storeTime
						options.storeTime ? SPATIAL_TEMPORAL_FIELDS : fields),
				new ByteArrayId(//TODO append CRS code to ID if its overridden 
						options.storeTime ? DEFAULT_SPATIAL_ID + "_TIME" : DEFAULT_SPATIAL_ID));
	}

	public static class SpatialOptions implements
			DimensionalityTypeOptions
	{
		@Parameter(names = {
			"--storeTime"
		}, required = false, description = "The index will store temporal values.  This allows it to slightly more efficiently run spatial-temporal queries although if spatial-temporal queries are a common use case, a separate spatial-temporal index is recommended.")
		protected boolean storeTime = false;
		
		@Parameter(names = {
				"-c","--crs"
			}, required = false, description = "The native Coordinate Reference System used within the index.  All spatial data will be projected into this CRS for appropriate indexing as needed.")
			protected String crs = GeometryUtils.DEFAULT_CRS_STR ;
	}

	@Override
	public Class<? extends CommonIndexValue>[] getRequiredIndexTypes() {
		return new Class[] {
			GeometryWrapper.class
		};
	}

	public static class SpatialIndexBuilder extends
			BaseIndexBuilder<SpatialIndexBuilder>
	{
		private final SpatialOptions options;

		public SpatialIndexBuilder() {
			super();
			options = new SpatialOptions();
		}

		public SpatialIndexBuilder setIncludeTimeInCommonIndexModel(
				final boolean storeTime ) {
			options.storeTime = storeTime;
			return this;
		}

		@Override
		public PrimaryIndex createIndex() {
			return createIndex(internalCreatePrimaryIndex(options));
		}
	}

	public static boolean isSpatial(
			final PrimaryIndex index ) {
		if ((index == null) || (index.getIndexStrategy() == null)
				|| (index.getIndexStrategy().getOrderedDimensionDefinitions() == null)) {
			return false;
		}
		final NumericDimensionDefinition[] dimensions = index.getIndexStrategy().getOrderedDimensionDefinitions();
		if (dimensions.length != 2) {
			return false;
		}
		boolean hasLat = false, hasLon = false;
		for (final NumericDimensionDefinition definition : dimensions) {
			if (definition instanceof LatitudeDefinition) {
				hasLat = true;
			}
			else if (definition instanceof LongitudeDefinition) {
				hasLon = true;
			}
		}
		return hasLat && hasLon;
	}

}
