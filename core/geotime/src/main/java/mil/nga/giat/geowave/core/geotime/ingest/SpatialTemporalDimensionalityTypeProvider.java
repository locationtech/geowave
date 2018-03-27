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

import java.util.Locale;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.store.dimension.CustomCRSSpatialDimension;
import mil.nga.giat.geowave.core.geotime.store.dimension.CustomCRSSpatialField;
import mil.nga.giat.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;

import mil.nga.giat.geowave.core.index.sfc.xz.XZHierarchicalIndexFactory;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions.BaseIndexBuilder;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeProviderSpi;

import org.apache.commons.lang3.StringUtils;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public class SpatialTemporalDimensionalityTypeProvider implements
		DimensionalityTypeProviderSpi<SpatialTemporalOptions>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SpatialTemporalDimensionalityTypeProvider.class);
	private static final String DEFAULT_SPATIAL_TEMPORAL_ID_STR = "SPATIAL_TEMPORAL_IDX";

	// TODO should we use different default IDs for all the different
	// options, for now lets just use one
	final static NumericDimensionField[] SPATIAL_TEMPORAL_FIELDS = new NumericDimensionField[] {
		new LongitudeField(),
		new LatitudeField(
				true),
		new TimeField(
				SpatialTemporalOptions.DEFAULT_PERIODICITY)
	};
	final static NumericDimensionDefinition[] SPATIAL_TEMPORAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new LongitudeDefinition(),
		new LatitudeDefinition(
				true),
		new TimeDefinition(
				SpatialTemporalOptions.DEFAULT_PERIODICITY)
	};

	public SpatialTemporalDimensionalityTypeProvider() {}

	@Override
	public String getDimensionalityTypeName() {
		return "spatial_temporal";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return "This dimensionality type matches all indices that only require Geometry and Time.";
	}

	@Override
	public int getPriority() {
		// arbitrary - just lower than spatial so that the default
		// will be spatial over spatial-temporal
		return 5;
	}

	@Override
	public SpatialTemporalOptions createOptions() {
		return new SpatialTemporalOptions();
	}

	@Override
	public PrimaryIndex createPrimaryIndex(
			SpatialTemporalOptions options ) {
		return internalCreatePrimaryIndex(options);
	}

	private static PrimaryIndex internalCreatePrimaryIndex(
			final SpatialTemporalOptions options ) {

		NumericDimensionDefinition[] dimensions;
		NumericDimensionField<?>[] fields = null;
		CoordinateReferenceSystem crs = null;
		boolean isDefaultCRS;
		String crsCode = null;

		if (options.crs == null || options.crs.isEmpty() || options.crs.equalsIgnoreCase(GeometryUtils.DEFAULT_CRS_STR)) {
			dimensions = SPATIAL_TEMPORAL_DIMENSIONS;
			fields = SPATIAL_TEMPORAL_FIELDS;
			isDefaultCRS = true;
			crsCode = "EPSG:4326";
		}
		else {
			crs = decodeCRS(options.crs);
			CoordinateSystem cs = crs.getCoordinateSystem();
			isDefaultCRS = false;
			crsCode = options.crs;
			dimensions = new NumericDimensionDefinition[cs.getDimension() + 1];
			fields = new NumericDimensionField[dimensions.length];

			for (int d = 0; d < dimensions.length - 1; d++) {
				CoordinateSystemAxis csa = cs.getAxis(d);
				dimensions[d] = new CustomCRSSpatialDimension(
						(byte) d,
						csa.getMinimumValue(),
						csa.getMaximumValue());
				fields[d] = new CustomCRSSpatialField(
						(CustomCRSSpatialDimension) dimensions[d]);
			}

			dimensions[dimensions.length - 1] = new TimeDefinition(
					options.periodicity);
			fields[dimensions.length - 1] = new TimeField(
					options.periodicity);
		}

		BasicIndexModel indexModel = null;
		if (isDefaultCRS) {
			indexModel = new BasicIndexModel(
					fields);
		}
		else {
			indexModel = new CustomCrsIndexModel(
					fields,
					crsCode);
		}

		String combinedArrayID;
		if (isDefaultCRS) {
			combinedArrayID = DEFAULT_SPATIAL_TEMPORAL_ID_STR + "_" + options.bias + "_" + options.periodicity;
		}
		else {
			combinedArrayID = DEFAULT_SPATIAL_TEMPORAL_ID_STR + "_" + (crsCode.substring(crsCode.indexOf(":") + 1))
					+ "_" + options.bias + "_" + options.periodicity;
		}
		final String combinedId = combinedArrayID;

		return new CustomIdIndex(
				XZHierarchicalIndexFactory.createFullIncrementalTieredStrategy(
						dimensions,
						new int[] {
							options.bias.getSpatialPrecision(),
							options.bias.getSpatialPrecision(),
							options.bias.getTemporalPrecision()
						},
						SFCType.HILBERT,
						options.maxDuplicates),
				indexModel,
				new ByteArrayId(
						combinedId));
	}

	public static CoordinateReferenceSystem decodeCRS(
			String crsCode ) {

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
					"Unable to decode '" + crsCode + "' CRS",
					e);
		}

		return crs;

	}

	@Override
	public Class<? extends CommonIndexValue>[] getRequiredIndexTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}

	public static enum Bias {
		TEMPORAL,
		BALANCED,
		SPATIAL;
		// converter that will be used later
		public static Bias fromString(
				final String code ) {

			for (final Bias output : Bias.values()) {
				if (output.toString().equalsIgnoreCase(
						code)) {
					return output;
				}
			}

			return null;
		}

		protected int getSpatialPrecision() {
			switch (this) {
				case SPATIAL:
					return 25;
				case TEMPORAL:
					return 10;
				case BALANCED:
				default:
					return 20;
			}
		}

		protected int getTemporalPrecision() {
			switch (this) {
				case SPATIAL:
					return 10;
				case TEMPORAL:
					return 40;
				case BALANCED:
				default:
					return 20;
			}
		}
	}

	public static class BiasConverter implements
			IStringConverter<Bias>
	{
		@Override
		public Bias convert(
				final String value ) {
			final Bias convertedValue = Bias.fromString(value);

			if (convertedValue == null) {
				throw new ParameterException(
						"Value " + value + "can not be converted to an index bias. " + "Available values are: "
								+ StringUtils.join(
										Bias.values(),
										", ").toLowerCase(
										Locale.ENGLISH));
			}
			return convertedValue;
		}

	}

	public static class UnitConverter implements
			IStringConverter<Unit>
	{

		@Override
		public Unit convert(
				final String value ) {
			final Unit convertedValue = Unit.fromString(value);

			if (convertedValue == null) {
				throw new ParameterException(
						"Value " + value + "can not be converted to Unit. " + "Available values are: "
								+ StringUtils.join(
										Unit.values(),
										", ").toLowerCase(
										Locale.ENGLISH));
			}
			return convertedValue;
		}
	}

	public static class SpatialTemporalIndexBuilder extends
			BaseIndexBuilder<SpatialTemporalIndexBuilder>
	{
		private final SpatialTemporalOptions options;

		public SpatialTemporalIndexBuilder() {
			options = new SpatialTemporalOptions();
		}

		public SpatialTemporalIndexBuilder setBias(
				final Bias bias ) {
			options.bias = bias;
			return this;
		}

		public SpatialTemporalIndexBuilder setPeriodicity(
				final Unit periodicity ) {
			options.periodicity = periodicity;
			return this;
		}

		public SpatialTemporalIndexBuilder setMaxDuplicates(
				final long maxDuplicates ) {
			options.maxDuplicates = maxDuplicates;
			return this;
		}
		
		public SpatialTemporalIndexBuilder setCrs(
				final String crs ) {
			options.crs = crs;
			return this;
		}

		@Override
		public PrimaryIndex createIndex() {
			return createIndex(internalCreatePrimaryIndex(options));
		}
	}

	public static boolean isSpatialTemporal(
			final PrimaryIndex index ) {
		if ((index == null) || (index.getIndexStrategy() == null)
				|| (index.getIndexStrategy().getOrderedDimensionDefinitions() == null)) {
			return false;
		}
		final NumericDimensionDefinition[] dimensions = index.getIndexStrategy().getOrderedDimensionDefinitions();
		if (dimensions.length < 3) {
			return false;
		}
		boolean hasLat = false, hasLon = false, hasTime = false;
		for (final NumericDimensionDefinition definition : dimensions) {
			if (definition instanceof TimeDefinition) {
				hasTime = true;
			}
			else if (definition instanceof LatitudeDefinition) {
				hasLat = true;
			}
			else if (definition instanceof LongitudeDefinition) {
				hasLon = true;
			}
		}
		return hasTime && hasLat && hasLon;
	}
}
