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
package org.locationtech.geowave.core.geotime.ingest;

import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSBoundedSpatialDimension;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSSpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimension;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimensionX;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimensionY;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.geotime.store.dimension.LatitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.LongitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.xz.XZHierarchicalIndexFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions.BaseIndexBuilder;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.BasicIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.index.CustomNameIndex;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeProviderSpi;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpatialDimensionalityTypeProvider implements
		DimensionalityTypeProviderSpi<SpatialOptions>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SpatialDimensionalityTypeProvider.class);
	private static final String DEFAULT_SPATIAL_ID = "SPATIAL_IDX";
	public static final int LONGITUDE_BITS = 31;
	public static final int LATITUDE_BITS = 31;
	// this is chosen to place metric CRSs always in the same bin
	public static final double DEFAULT_UNBOUNDED_CRS_INTERVAL = 40075017;

	public static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new LongitudeDefinition(),
		new LatitudeDefinition(
				true)
	// just use the same range for latitude to make square sfc values in
	// decimal degrees (EPSG:4326)
	};
	public static final NumericDimensionField[] SPATIAL_FIELDS = new NumericDimensionField[] {
		new LongitudeField(),
		new LatitudeField(
				true)
	// just use the same range for latitude to make square sfc values in
	// decimal degrees (EPSG:4326)
	};
	public static final NumericDimensionField[] SPATIAL_TEMPORAL_FIELDS = new NumericDimensionField[] {
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
	public SpatialOptions createOptions() {
		return new SpatialOptions();
	}

	@Override
	public Index createIndex(
			final SpatialOptions options ) {
		return internalCreateIndex(options);
	}

	private static Index internalCreateIndex(
			final SpatialOptions options ) {
		NumericDimensionDefinition[] dimensions;
		boolean isDefaultCRS;
		String crsCode = null;
		NumericDimensionField<?>[] fields = null;
		NumericDimensionField<?>[] fields_temporal = null;

		if ((options.crs == null) || options.crs.isEmpty()
				|| options.crs.equalsIgnoreCase(GeometryUtils.DEFAULT_CRS_STR)) {
			dimensions = SPATIAL_DIMENSIONS;
			fields = SPATIAL_FIELDS;
			isDefaultCRS = true;
			crsCode = "EPSG:4326";
		}
		else {
			decodeCRS(options.crs);
			final CoordinateSystem cs = decodeCRS(
					options.crs).getCoordinateSystem();
			isDefaultCRS = false;
			crsCode = options.crs;
			dimensions = new NumericDimensionDefinition[cs.getDimension()];
			if (options.storeTime) {
				fields_temporal = new NumericDimensionField[dimensions.length + 1];
				for (int d = 0; d < dimensions.length; d++) {
					final CoordinateSystemAxis csa = cs.getAxis(d);
					if (!isUnbounded(csa)) {
						dimensions[d] = new CustomCRSBoundedSpatialDimension(
								(byte) d,
								csa.getMinimumValue(),
								csa.getMaximumValue());
						fields_temporal[d] = new CustomCRSSpatialField(
								(CustomCRSBoundedSpatialDimension) dimensions[d]);
					}
					else {
						dimensions[d] = new CustomCRSUnboundedSpatialDimension(
								DEFAULT_UNBOUNDED_CRS_INTERVAL,
								(byte) d);
						fields_temporal[d] = new CustomCRSSpatialField(
								(CustomCRSUnboundedSpatialDimension) dimensions[d]);
					}
				}
				fields_temporal[dimensions.length] = new TimeField(
						Unit.YEAR);
			}
			else {
				fields = new NumericDimensionField[dimensions.length];
				for (int d = 0; d < dimensions.length; d++) {
					final CoordinateSystemAxis csa = cs.getAxis(d);
					if (!isUnbounded(csa)) {
						dimensions[d] = new CustomCRSBoundedSpatialDimension(
								(byte) d,
								csa.getMinimumValue(),
								csa.getMaximumValue());
						fields[d] = new CustomCRSSpatialField(
								(CustomCRSBoundedSpatialDimension) dimensions[d]);
					}
					else {
						if (d == 0) {
							dimensions[d] = new CustomCRSUnboundedSpatialDimensionX(
									DEFAULT_UNBOUNDED_CRS_INTERVAL,
									(byte) d);
							fields[d] = new CustomCRSSpatialField(
									(CustomCRSUnboundedSpatialDimensionX) dimensions[d]);
						}
						if (d == 1) {
							dimensions[d] = new CustomCRSUnboundedSpatialDimensionY(
									DEFAULT_UNBOUNDED_CRS_INTERVAL,
									(byte) d);
							fields[d] = new CustomCRSSpatialField(
									(CustomCRSUnboundedSpatialDimensionY) dimensions[d]);
						}
					}
				}
			}

		}

		BasicIndexModel indexModel = null;
		if (isDefaultCRS) {
			indexModel = new BasicIndexModel(
					options.storeTime ? SPATIAL_TEMPORAL_FIELDS : SPATIAL_FIELDS);
		}
		else {

			indexModel = new CustomCrsIndexModel(
					options.storeTime ? fields_temporal : fields,
					crsCode);
		}

		return new CustomNameIndex(
				XZHierarchicalIndexFactory.createFullIncrementalTieredStrategy(
						dimensions,
						new int[] {
							// TODO this is only valid for 2D coordinate
							// systems, again consider the possibility
							// of being
							// flexible enough to handle n-dimensions
							LONGITUDE_BITS,
							LATITUDE_BITS
						},
						SFCType.HILBERT),
				indexModel,
				// TODO append CRS code to ID if its overridden
				isDefaultCRS ? (options.storeTime ? DEFAULT_SPATIAL_ID + "_TIME" : DEFAULT_SPATIAL_ID)
						: (options.storeTime ? DEFAULT_SPATIAL_ID + "_TIME" : DEFAULT_SPATIAL_ID) + "_"
								+ crsCode.substring(crsCode.indexOf(":") + 1));
	}

	private static boolean isUnbounded(
			final CoordinateSystemAxis csa ) {
		final double min = csa.getMinimumValue();
		final double max = csa.getMaximumValue();

		if (!Double.isFinite(max) || !Double.isFinite(min)) {
			return true;
		}
		return false;
	}

	public static CoordinateReferenceSystem decodeCRS(
			final String crsCode ) {

		CoordinateReferenceSystem crs = null;
		try {
			crs = CRS.decode(
					crsCode,
					true);
		}
		catch (final FactoryException e) {
			LOGGER.error(
					"Unable to decode '" + crsCode + "' CRS",
					e);
			throw new RuntimeException(
					"Unable to initialize '" + crsCode + "' object",
					e);
		}

		return crs;

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

		public SpatialIndexBuilder setCrs(
				final String crs ) {
			options.crs = crs;
			return this;
		}

		@Override
		public Index createIndex() {
			return createIndex(internalCreateIndex(options));
		}
	}

	public static boolean isSpatial(
			final Index index ) {
		if (index == null) {
			return false;
		}

		return isSpatial(index.getIndexStrategy());
	}

	public static boolean isSpatial(
			final NumericIndexStrategy indexStrategy ) {
		if ((indexStrategy == null) || (indexStrategy.getOrderedDimensionDefinitions() == null)) {
			return false;
		}
		final NumericDimensionDefinition[] dimensions = indexStrategy.getOrderedDimensionDefinitions();
		if (dimensions.length != 2) {
			return false;
		}
		boolean hasLat = false, hasLon = false;
		for (final NumericDimensionDefinition definition : dimensions) {
			if ((definition instanceof LatitudeDefinition)
					|| (definition instanceof CustomCRSUnboundedSpatialDimensionY)) {
				hasLat = true;
			}
			else if ((definition instanceof LongitudeDefinition)
					|| (definition instanceof CustomCRSUnboundedSpatialDimensionX)) {
				hasLon = true;
			}
		}
		return hasLat && hasLon;
	}

}
