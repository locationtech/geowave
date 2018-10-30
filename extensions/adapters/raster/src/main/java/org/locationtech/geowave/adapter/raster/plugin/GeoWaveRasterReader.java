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
package org.locationtech.geowave.adapter.raster.plugin;

import java.awt.Color;
import java.awt.Rectangle;
import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.imageio.ImageReadParam;
import javax.media.jai.Histogram;
import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.OverviewPolicy;
import org.geotools.data.DataSourceException;
import org.geotools.factory.Hints;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.parameter.Parameter;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.BufferedCoordinateOperationFactory;
import org.geotools.util.Utilities;
import org.locationtech.geowave.adapter.auth.AuthorizationSPI;
import org.locationtech.geowave.adapter.raster.RasterUtils;
import org.locationtech.geowave.adapter.raster.Resolution;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.stats.HistogramStatistics;
import org.locationtech.geowave.adapter.raster.stats.OverviewStatistics;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.store.query.IndexOnlySpatialQuery;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIterator.Wrapper;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.opengis.coverage.grid.Format;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.coverage.grid.GridEnvelope;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.AxisDirection;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.CoordinateOperationFactory;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * the reader gets the connection info and returns a grid coverage for every
 * data adapter
 */
public class GeoWaveRasterReader extends
		AbstractGridCoverage2DReader implements
		GridCoverage2DReader
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveRasterReader.class);

	private GeoWaveRasterConfig config;

	private PersistentAdapterStore geowaveAdapterStore;

	private InternalAdapterStore geowaveInternalAdapterStore;

	private DataStatisticsStore geowaveStatisticsStore;

	private DataStore geowaveDataStore;

	private IndexStore geowaveIndexStore;

	private AdapterIndexMappingStore geowaveAdapterIndexMappingStore;
	protected Map<String, CoordinateReferenceSystem> crsCache = new HashMap<>();
	protected CoordinateReferenceSystem defaultCrs;

	private AuthorizationSPI authorizationSPI;

	protected final static CoordinateOperationFactory OPERATION_FACTORY = new BufferedCoordinateOperationFactory(
			new Hints(
					Hints.LENIENT_DATUM_SHIFT,
					Boolean.TRUE));
	private static Set<AxisDirection> UPDirections;

	private static Set<AxisDirection> LEFTDirections;
	// class initializer
	static {
		LEFTDirections = new HashSet<>();
		LEFTDirections.add(AxisDirection.DISPLAY_LEFT);
		LEFTDirections.add(AxisDirection.EAST);
		LEFTDirections.add(AxisDirection.GEOCENTRIC_X);
		LEFTDirections.add(AxisDirection.COLUMN_POSITIVE);

		UPDirections = new HashSet<>();
		UPDirections.add(AxisDirection.DISPLAY_UP);
		UPDirections.add(AxisDirection.NORTH);
		UPDirections.add(AxisDirection.GEOCENTRIC_Y);
		UPDirections.add(AxisDirection.ROW_POSITIVE);

	}

	/**
	 * @param source
	 *            The source object.
	 * @param uHints
	 * @throws IOException
	 * @throws MalformedURLException
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 */
	public GeoWaveRasterReader(
			final Object source,
			final Hints uHints )
			throws IOException {
		super(
				source,
				uHints);
		this.source = source;
		if (GeoWaveGTRasterFormat.isParamList(source)) {
			try {
				config = GeoWaveRasterConfig.readFromConfigParams(source.toString());
			}
			catch (final Exception e) {
				throw new MalformedURLException(
						source.toString());
			}
		}
		else {
			final URL url = GeoWaveGTRasterFormat.getURLFromSource(source);

			if (url == null) {
				throw new MalformedURLException(
						source.toString());
			}

			try {
				config = GeoWaveRasterConfig.readFromURL(url);
			}
			catch (final Exception e) {
				LOGGER.error(
						"Cannot read config",
						e);
				throw new IOException(
						e);
			}
		}
		init(config);
	}

	public GeoWaveRasterReader(
			final GeoWaveRasterConfig config )
			throws DataSourceException {
		super(
				new Object(),
				new Hints());
		this.config = config;
		init(config);
	}

	private void init(
			final GeoWaveRasterConfig config ) {

		geowaveDataStore = config.getDataStore();
		geowaveAdapterStore = config.getAdapterStore();
		geowaveStatisticsStore = config.getDataStatisticsStore();
		geowaveIndexStore = config.getIndexStore();
		geowaveAdapterIndexMappingStore = config.getAdapterIndexMappingStore();
		geowaveInternalAdapterStore = config.getInternalAdapterStore();
		authorizationSPI = config.getAuthorizationFactory().create(
				config.getAuthorizationURL());

	}

	/**
	 * Constructor.
	 *
	 * @param source
	 *            The source object.
	 * @throws IOException
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 * @throws UnsupportedEncodingException
	 *
	 */
	public GeoWaveRasterReader(
			final Object source )
			throws IOException {
		this(
				source,
				null);
	}

	protected CoordinateReferenceSystem getDefaultCrs() {
		if (defaultCrs != null) {
			return defaultCrs;
		}
		if (!crsCache.isEmpty()) {
			defaultCrs = crsCache.values().iterator().next();
		}
		else {
			final String[] coverageNames = getGridCoverageNames();
			for (final String coverageName : coverageNames) {
				final CoordinateReferenceSystem crs = getCrsForCoverage(coverageName);
				if (crs != null) {
					defaultCrs = crs;
					break;
				}
			}
		}
		if (defaultCrs != null) {
			return defaultCrs;
		}
		// if no data has been ingested yet with a CRS, this is the best guess
		// we can make
		return GeometryUtils.getDefaultCRS();
	}

	protected CoordinateReferenceSystem getCrsForCoverage(
			final String coverageName ) {
		CoordinateReferenceSystem crs = crsCache.get(coverageName);
		if (crs != null) {
			return crs;
		}

		final AdapterToIndexMapping adapterMapping = geowaveAdapterIndexMappingStore
				.getIndicesForAdapter(getAdapterId(coverageName));
		final Index[] indices = adapterMapping.getIndices(geowaveIndexStore);

		if ((indices != null) && (indices.length > 0)) {
			crs = GeometryUtils.getIndexCrs(indices[0]);
			crsCache.put(
					coverageName,
					crs);
		}
		return crs;
	}

	@Override
	public Format getFormat() {
		return new GeoWaveGTRasterFormat();
	}

	@Override
	public String[] getGridCoverageNames() {
		try (final CloseableIterator<InternalDataAdapter<?>> it = geowaveAdapterStore.getAdapters()) {
			final List<String> coverageNames = new ArrayList<>();
			while (it.hasNext()) {
				final DataTypeAdapter<?> adapter = it.next().getAdapter();
				if (adapter instanceof RasterDataAdapter) {
					coverageNames.add(((RasterDataAdapter) adapter).getCoverageName());
				}
			}
			return coverageNames.toArray(new String[coverageNames.size()]);
		}
	}

	@Override
	public int getGridCoverageCount() {
		try (final CloseableIterator<InternalDataAdapter<?>> it = geowaveAdapterStore.getAdapters()) {
			int coverageCount = 0;
			while (it.hasNext()) {
				final DataTypeAdapter<?> adapter = it.next().getAdapter();
				if (adapter instanceof RasterDataAdapter) {
					coverageCount++;
				}
			}
			return coverageCount;
		}
	}

	@Override
	public String[] getMetadataNames() {
		throw new UnsupportedOperationException(
				"A coverage name must be provided, there is no support for a default coverage");
	}

	@Override
	public String[] getMetadataNames(
			final String coverageName ) {
		if (!checkName(coverageName)) {
			LOGGER.warn("Unable to find data adapter for '" + coverageName + "'");
			return null;
		}

		final DataTypeAdapter<?> adapter = geowaveAdapterStore.getAdapter(
				getAdapterId(coverageName)).getAdapter();
		final Set<String> var = ((RasterDataAdapter) adapter).getMetadata().keySet();
		return var.toArray(new String[var.size()]);
	}

	@Override
	public String getMetadataValue(
			final String name ) {
		throw new UnsupportedOperationException(
				"A coverage name must be provided, there is no support for a default coverage");
	}

	@Override
	public String getMetadataValue(
			final String coverageName,
			final String name ) {
		if (!checkName(coverageName)) {
			LOGGER.warn("Unable to find data adapter for '" + coverageName + "'");
			return null;
		}

		final DataTypeAdapter<?> adapter = geowaveAdapterStore.getAdapter(
				getAdapterId(coverageName)).getAdapter();

		return ((RasterDataAdapter) adapter).getMetadata().get(
				name);
	}

	@Override
	protected boolean checkName(
			final String coverageName ) {
		Utilities.ensureNonNull(
				"coverageName",
				coverageName);

		final DataTypeAdapter<?> adapter = geowaveAdapterStore.getAdapter(
				getAdapterId(coverageName)).getAdapter();
		return (adapter != null) && (adapter instanceof RasterDataAdapter);
	}

	@Override
	public GeneralEnvelope getOriginalEnvelope() {
		throw new UnsupportedOperationException(
				"A coverage name must be provided, there is no support for a default coverage");
	}

	@Override
	public GeneralEnvelope getOriginalEnvelope(
			final String coverageName ) {
		final Envelope envelope = geowaveDataStore.aggregateStatistics(BoundingBoxDataStatistics.STATS_TYPE
				.newBuilder()
				.setAuthorizations(
						authorizationSPI.getAuthorizations())
				.dataType(
						coverageName)
				.build());
		if (envelope == null) {
			CoordinateReferenceSystem crs = getCoordinateReferenceSystem(coverageName);
			double minX = crs.getCoordinateSystem().getAxis(
					0).getMinimumValue();
			double maxX = crs.getCoordinateSystem().getAxis(
					0).getMaximumValue();
			double minY = crs.getCoordinateSystem().getAxis(
					1).getMinimumValue();
			double maxY = crs.getCoordinateSystem().getAxis(
					1).getMaximumValue();
			final GeneralEnvelope env = new GeneralEnvelope(
					new Rectangle2D.Double(
							minX,
							minY,
							maxX - minX,
							maxY - minY));
			env.setCoordinateReferenceSystem(crs);
			return env;
		}
		// try to use both the bounding box and the overview statistics to
		// determine the width and height at the highest resolution
		final GeneralEnvelope env = new GeneralEnvelope(
				new Rectangle2D.Double(
						envelope.getMinX(),
						envelope.getMinY(),
						envelope.getWidth(),
						envelope.getHeight()));
		env.setCoordinateReferenceSystem(getCoordinateReferenceSystem(coverageName));
		return env;
	}

	@Override
	public CoordinateReferenceSystem getCoordinateReferenceSystem() {
		return getDefaultCrs();
	}

	@Override
	public CoordinateReferenceSystem getCoordinateReferenceSystem(
			final String coverageName ) {
		return getCrsForCoverage(coverageName);
	}

	@Override
	public GridEnvelope getOriginalGridRange() {
		throw new UnsupportedOperationException(
				"A coverage name must be provided, there is no support for a default coverage");
	}

	@Override
	public GridEnvelope getOriginalGridRange(
			final String coverageName ) {
		try (CloseableIterator<InternalDataStatistics<?, ?, ?>> statisticsIt = geowaveStatisticsStore
				.getDataStatistics(
						getAdapterId(coverageName),
						BoundingBoxDataStatistics.STATS_TYPE,
						authorizationSPI.getAuthorizations())) {

			int width = 0;
			int height = 0;
			// try to use both the bounding box and the overview statistics to
			// determine the width and height at the highest resolution
			InternalDataStatistics<?, ?, ?> statistics = null;
			if (statisticsIt.hasNext()) {
				statistics = statisticsIt.next();
			}
			if ((statistics != null) && (statistics instanceof BoundingBoxDataStatistics)) {
				final BoundingBoxDataStatistics<?> bboxStats = (BoundingBoxDataStatistics<?>) statistics;
				try (CloseableIterator<InternalDataStatistics<?, ?, ?>> overviewStatisticsIt = geowaveStatisticsStore
						.getDataStatistics(
								getAdapterId(coverageName),
								OverviewStatistics.STATS_TYPE,
								authorizationSPI.getAuthorizations())) {
					statistics = null;
					if (overviewStatisticsIt.hasNext()) {
						statistics = overviewStatisticsIt.next();
					}
					if ((statistics != null) && (statistics instanceof OverviewStatistics)) {
						final OverviewStatistics overviewStats = (OverviewStatistics) statistics;
						width = (int) Math.ceil(((bboxStats.getMaxX() - bboxStats.getMinX()) / overviewStats
								.getResolutions()[0].getResolution(0)));
						height = (int) Math.ceil(((bboxStats.getMaxY() - bboxStats.getMinY()) / overviewStats
								.getResolutions()[0].getResolution(1)));
					}
				}
			}

			return new GridEnvelope2D(
					0,
					0,
					width,
					height);
		}
	}

	@Override
	public MathTransform getOriginalGridToWorld(
			final PixelInCell pixInCell ) {
		throw new UnsupportedOperationException(
				"A coverage name must be provided, there is no support for a default coverage");
	}

	@Override
	public MathTransform getOriginalGridToWorld(
			final String coverageName,
			final PixelInCell pixInCell ) {
		// just reuse super class implementation but ensure that we do not use a
		// cached raster2model
		synchronized (this) {
			raster2Model = null;
			return super.getOriginalGridToWorld(
					coverageName,
					pixInCell);
		}
	}

	@Override
	public GridCoverage2D read(
			final GeneralParameterValue[] parameters )
			throws IllegalArgumentException,
			IOException {
		throw new UnsupportedOperationException(
				"A coverage name must be provided, there is no support for a default coverage");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.opengis.coverage.grid.GridCoverageReader#read(org.opengis.parameter
	 * .GeneralParameterValue [])
	 */
	@Override
	public GridCoverage2D read(
			final String coverageName,
			final GeneralParameterValue[] params )
			throws IOException {
		if (!checkName(coverageName)) {
			LOGGER.warn("Unable to find data adapter for '" + coverageName + "'");
			return null;
		}
		final Date start = new Date();
		// /////////////////////////////////////////////////////////////////////
		//
		// Checking params
		//
		// /////////////////////////////////////////////////////////////////////
		Color outputTransparentColor = null;

		Color backgroundColor = null;

		Interpolation interpolation = null;

		Rectangle dim = null;

		GeneralEnvelope requestedEnvelope = null;

		if (params != null) {
			for (final GeneralParameterValue generalParameterValue : params) {
				final Parameter<Object> param = (Parameter<Object>) generalParameterValue;

				if (param.getDescriptor().getName().getCode().equals(
						AbstractGridFormat.READ_GRIDGEOMETRY2D.getName().toString())) {
					final GridGeometry2D gg = (GridGeometry2D) param.getValue();
					requestedEnvelope = (GeneralEnvelope) gg.getEnvelope();
					dim = gg.getGridRange2D().getBounds();
				}
				else if (param.getDescriptor().getName().getCode().equals(
						GeoWaveGTRasterFormat.OUTPUT_TRANSPARENT_COLOR.getName().toString())) {
					outputTransparentColor = (Color) param.getValue();
				}
				else if (param.getDescriptor().getName().getCode().equals(
						AbstractGridFormat.BACKGROUND_COLOR.getName().toString())) {
					backgroundColor = (Color) param.getValue();
				}
				else if (param.getDescriptor().getName().getCode().equals(
						AbstractGridFormat.INTERPOLATION.getName().toString())) {
					interpolation = (Interpolation) param.getValue();
				}
			}
		}

		final GridCoverage2D coverage = renderGridCoverage(
				coverageName,
				dim,
				requestedEnvelope,
				backgroundColor,
				outputTransparentColor,
				interpolation);
		LOGGER.info("GeoWave Raster Reader needs : " + ((new Date()).getTime() - start.getTime()) + " millisecs");
		return coverage;
	}

	public GridCoverage2D renderGridCoverage(
			final String coverageName,
			final Rectangle dim,
			final GeneralEnvelope generalEnvelope,
			Color backgroundColor,
			Color outputTransparentColor,
			final Interpolation interpolation )
			throws IOException {
		if (backgroundColor == null) {
			backgroundColor = AbstractGridFormat.BACKGROUND_COLOR.getDefaultValue();
		}
		if (outputTransparentColor == null) {
			outputTransparentColor = GeoWaveGTRasterFormat.OUTPUT_TRANSPARENT_COLOR.getDefaultValue();
		}

		final GeoWaveRasterReaderState state = new GeoWaveRasterReaderState(
				coverageName);
		state.setRequestedEnvelope(generalEnvelope);
		// /////////////////////////////////////////////////////////////////////
		//
		// Loading tiles trying to optimize as much as possible
		//
		// /////////////////////////////////////////////////////////////////////
		final GridCoverage2D coverage = loadTiles(
				coverageName,
				backgroundColor,
				outputTransparentColor,
				interpolation,
				dim,
				state,
				getCoordinateReferenceSystem(coverageName),
				getOriginalEnvelope(coverageName));

		return coverage;
	}

	/**
	 * @param backgroundColor
	 *            the background color
	 * @param outputTransparentColor
	 *            the transparent color
	 * @param pixelDimension
	 * @return the gridcoverage as the final result
	 * @throws IOException
	 */
	private GridCoverage2D loadTiles(
			final String coverageName,
			final Color backgroundColor,
			final Color outputTransparentColor,
			Interpolation interpolation,
			final Rectangle pixelDimension,
			final GeoWaveRasterReaderState state,
			final CoordinateReferenceSystem crs,
			final GeneralEnvelope originalEnvelope )
			throws IOException {
		transformRequestEnvelope(
				state,
				crs);

		// /////////////////////////////////////////////////////////////////////
		//
		// Check if we have something to load by intersecting the requested
		// envelope with the bounds of the data set. If not, give warning
		//
		// /////////////////////////////////////////////////////////////////////
		if (!state.getRequestEnvelopeXformed().intersects(
				originalEnvelope,
				true)) {
			LOGGER.warn("The requested envelope does not intersect the envelope of this mosaic");
			LOGGER.warn(state.getRequestEnvelopeXformed().toString());
			LOGGER.warn(originalEnvelope.toString());

			return null;
		}

		final ImageReadParam readP = new ImageReadParam();
		final Integer imageChoice;

		final RasterDataAdapter adapter = (RasterDataAdapter) geowaveAdapterStore.getAdapter(
				getAdapterId(coverageName)).getAdapter();
		if (pixelDimension != null) {
			try {
				synchronized (this) {
					if (!setupResolutions(coverageName)) {
						LOGGER.warn("Cannot find the overview statistics for the requested coverage name");
						return coverageFactory.create(
								coverageName,
								RasterUtils.getEmptyImage(
										(int) pixelDimension.getWidth(),
										(int) pixelDimension.getHeight(),
										backgroundColor,
										outputTransparentColor,
										adapter.getColorModel()),
								state.getRequestedEnvelope());
					}
					imageChoice = setReadParams(
							state.getCoverageName(),
							OverviewPolicy.getDefaultPolicy(),
							readP,
							state.getRequestEnvelopeXformed(),
							pixelDimension);

				}
				readP.setSourceSubsampling(
						1,
						1,
						0,
						0);
			}
			catch (final TransformException e) {
				LOGGER.error(
						e.getLocalizedMessage(),
						e);

				return coverageFactory.create(
						coverageName,
						RasterUtils.getEmptyImage(
								(int) pixelDimension.getWidth(),
								(int) pixelDimension.getHeight(),
								backgroundColor,
								outputTransparentColor,
								adapter.getColorModel()),
						state.getRequestedEnvelope());
			}
		}
		else {
			imageChoice = Integer.valueOf(0);
		}

		final double[][] resolutionLevels = getResolutionLevels(coverageName);
		final Histogram histogram;

		boolean equalizeHistogram;
		if (config.isEqualizeHistogramOverrideSet()) {
			equalizeHistogram = config.isEqualizeHistogramOverride();
		}
		else {
			equalizeHistogram = adapter.isEqualizeHistogram();
		}
		if (equalizeHistogram) {
			histogram = getHistogram(
					coverageName,
					resolutionLevels[imageChoice.intValue()][0],
					resolutionLevels[imageChoice.intValue()][1]);
		}
		else {
			histogram = null;
		}
		boolean scaleTo8Bit = true; // default to always scale to 8-bit

		final boolean scaleTo8BitSet = config.isScaleTo8BitSet();
		if (scaleTo8BitSet) {
			scaleTo8Bit = config.isScaleTo8Bit();
		}

		try (final CloseableIterator<GridCoverage> gridCoverageIt = queryForTiles(
				pixelDimension,
				state.getRequestEnvelopeXformed(),
				resolutionLevels[imageChoice.intValue()][0],
				resolutionLevels[imageChoice.intValue()][1],
				adapter)) {
			// allow the config to override the WMS request
			if (config.isInterpolationOverrideSet()) {
				interpolation = config.getInterpolationOverride();
			}
			// but don't allow the default adapter interpolation to override the
			// WMS request
			else if (interpolation == null) {
				interpolation = adapter.getInterpolation();
			}
			final GridCoverage2D result = RasterUtils.mosaicGridCoverages(
					gridCoverageIt,
					backgroundColor,
					outputTransparentColor,
					pixelDimension,
					state.getRequestEnvelopeXformed(),
					resolutionLevels[imageChoice.intValue()][0],
					resolutionLevels[imageChoice.intValue()][1],
					adapter.getNoDataValuesPerBand(),
					state.isAxisSwapped(),
					coverageFactory,
					state.getCoverageName(),
					interpolation,
					histogram,
					scaleTo8BitSet,
					scaleTo8Bit,
					adapter.getColorModel());

			return transformResult(
					result,
					pixelDimension,
					state);
		}
	}

	private boolean setupResolutions(
			final String coverageName )
			throws IOException {

		// this is a bit of a hack to avoid copy and pasting large
		// portions of the inherited class, which does not handle
		// multiple coverage names
		final double[][] resLevels = getResolutionLevels(coverageName);
		if (resLevels == null || resLevels.length == 0) {
			return false;
		}
		numOverviews = resLevels.length - 1;
		highestRes = resLevels[0];
		if (numOverviews > 0) {
			overViewResolutions = new double[numOverviews][];

			System.arraycopy(
					resLevels,
					1,
					overViewResolutions,
					0,
					numOverviews);
		}
		else {
			overViewResolutions = new double[][] {};
		}
		this.coverageName = coverageName;
		return true;
	}

	private CloseableIterator<GridCoverage> queryForTiles(
			final Rectangle pixelDimension,
			final GeneralEnvelope requestEnvelope,
			final double levelResX,
			final double levelResY,
			final RasterDataAdapter adapter )
			throws IOException {
		final QueryConstraints query;
		if (requestEnvelope.getCoordinateReferenceSystem() != null) {
			query = new IndexOnlySpatialQuery(
					new GeometryFactory().toGeometry(new Envelope(
							requestEnvelope.getMinimum(0),
							requestEnvelope.getMaximum(0),
							requestEnvelope.getMinimum(1),
							requestEnvelope.getMaximum(1))),
					GeometryUtils.getCrsCode(requestEnvelope.getCoordinateReferenceSystem()));
		}
		else {
			query = new IndexOnlySpatialQuery(
					new GeometryFactory().toGeometry(new Envelope(
							requestEnvelope.getMinimum(0),
							requestEnvelope.getMaximum(0),
							requestEnvelope.getMinimum(1),
							requestEnvelope.getMaximum(1))));
		}
		return queryForTiles(
				adapter,
				query,
				new double[] {
					levelResX * adapter.getTileSize(),
					levelResY * adapter.getTileSize()
				});
	}

	private CloseableIterator<GridCoverage> queryForTiles(
			final RasterDataAdapter adapter,
			final QueryConstraints query,
			final double[] targetResolutionPerDimension ) {
		final AdapterToIndexMapping adapterIndexMapping = geowaveAdapterIndexMappingStore
				.getIndicesForAdapter(getAdapterId(adapter.getTypeName()));
		final Index[] indices = adapterIndexMapping.getIndices(geowaveIndexStore);
		// just work on the first spatial only index that contains this adapter
		// ID
		// TODO consider the best strategy for handling temporal queries here
		for (final Index rasterIndex : indices) {
			if (SpatialDimensionalityTypeProvider.isSpatial(rasterIndex)) {
				return (CloseableIterator) geowaveDataStore.query(QueryBuilder.newBuilder().setAuthorizations(
						authorizationSPI.getAuthorizations()).addTypeName(
						adapter.getTypeName()).constraints(
						query).addHint(
						DataStoreUtils.TARGET_RESOLUTION_PER_DIMENSION_FOR_HIERARCHICAL_INDEX,
						targetResolutionPerDimension).build());
			}
		}
		return new Wrapper(
				Collections.emptyIterator());
	}

	private GridCoverage2D transformResult(
			final GridCoverage2D coverage,
			final Rectangle pixelDimension,
			final GeoWaveRasterReaderState state ) {
		if (state.getRequestEnvelopeXformed() == state.getRequestedEnvelope()) {
			return coverage; // nothing to do
		}

		GridCoverage2D result = null;
		LOGGER.info("Image reprojection necessary");
		result = (GridCoverage2D) RasterUtils.getCoverageOperations().resample(
				coverage,
				state.getRequestedEnvelope().getCoordinateReferenceSystem());

		return coverageFactory.create(
				result.getName(),
				result.getRenderedImage(),
				result.getEnvelope());

	}

	/**
	 * transforms (if necessary) the requested envelope into the CRS used by
	 * this reader.
	 *
	 * @throws DataSourceException
	 */
	public static void transformRequestEnvelope(
			final GeoWaveRasterReaderState state,
			final CoordinateReferenceSystem crs )
			throws DataSourceException {

		if (CRS.equalsIgnoreMetadata(
				state.getRequestedEnvelope().getCoordinateReferenceSystem(),
				crs)) {
			state.setRequestEnvelopeXformed(state.getRequestedEnvelope());

			return; // and finish
		}

		try {
			/** Buffered factory for coordinate operations. */

			// transforming the envelope back to the dataset crs in
			final MathTransform transform = OPERATION_FACTORY.createOperation(
					state.getRequestedEnvelope().getCoordinateReferenceSystem(),
					crs).getMathTransform();

			if (transform.isIdentity()) { // Identity Transform ?
				state.setRequestEnvelopeXformed(state.getRequestedEnvelope());
				return; // and finish
			}

			state.setRequestEnvelopeXformed(CRS.transform(
					transform,
					state.getRequestedEnvelope()));
			state.getRequestEnvelopeXformed().setCoordinateReferenceSystem(
					crs);

			// if (config.getIgnoreAxisOrder() == false) { // check for axis
			// order
			// required
			final int indexX = indexOfX(crs);
			final int indexY = indexOfY(crs);
			final int indexRequestedX = indexOfX(state.getRequestedEnvelope().getCoordinateReferenceSystem());
			final int indexRequestedY = indexOfY(state.getRequestedEnvelope().getCoordinateReferenceSystem());

			// x Axis problem ???
			if ((indexX == indexRequestedY) && (indexY == indexRequestedX)) {
				state.setAxisSwap(true);
				final Rectangle2D tmp = new Rectangle2D.Double(
						state.getRequestEnvelopeXformed().getMinimum(
								1),
						state.getRequestEnvelopeXformed().getMinimum(
								0),
						state.getRequestEnvelopeXformed().getSpan(
								1),
						state.getRequestEnvelopeXformed().getSpan(
								0));
				state.setRequestEnvelopeXformed(new GeneralEnvelope(
						tmp));
				state.getRequestEnvelopeXformed().setCoordinateReferenceSystem(
						crs);
			}
			else if ((indexX == indexRequestedX) && (indexY == indexRequestedY)) {
				// everything is fine
			}
			else {
				throw new DataSourceException(
						"Unable to resolve the X Axis problem");
			}
			// }
		}
		catch (final Exception e) {
			throw new DataSourceException(
					"Unable to create a coverage for this source",
					e);
		}
	}

	@Override
	public Set<ParameterDescriptor<List>> getDynamicParameters()
			throws IOException {
		throw new UnsupportedOperationException(
				"A coverage name must be provided, there is no support for a default coverage");
	}

	@Override
	public Set<ParameterDescriptor<List>> getDynamicParameters(
			final String coverageName )
			throws IOException {
		return Collections.emptySet();
	}

	@Override
	public double[] getReadingResolutions(
			final OverviewPolicy policy,
			final double[] requestedResolution )
			throws IOException {
		throw new UnsupportedOperationException(
				"A coverage name must be provided, there is no support for a default coverage");
	}

	@Override
	public double[] getReadingResolutions(
			final String coverageName,
			final OverviewPolicy policy,
			final double[] requestedResolution )
			throws IOException {
		synchronized (this) {
			if (!setupResolutions(coverageName)) {
				LOGGER.warn("Cannot find the overview statistics for the requested coverage name");
				return null;
			}
			return super.getReadingResolutions(
					coverageName,
					policy,
					requestedResolution);
		}
	}

	@Override
	public int getNumOverviews() {
		throw new UnsupportedOperationException(
				"A coverage name must be provided, there is no support for a default coverage");
	}

	@Override
	public int getNumOverviews(
			final String coverageName ) {
		try {
			final double[][] resolutionLevels = getResolutionLevels(coverageName);
			return Math.max(
					0,
					resolutionLevels.length - 1);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read resolution levels",
					e);
		}
		return 0;
	}

	@Override
	public ImageLayout getImageLayout()
			throws IOException {
		throw new UnsupportedOperationException(
				"A coverage name must be provided, there is no support for a default coverage");
	}

	@Override
	public ImageLayout getImageLayout(
			final String coverageName )
			throws IOException {
		if (!checkName(coverageName)) {
			LOGGER.warn("Unable to find data adapter for '" + coverageName + "'");
			return null;
		}

		final RasterDataAdapter adapter = (RasterDataAdapter) geowaveAdapterStore
				.getAdapter(getAdapterId(coverageName));
		final GridEnvelope gridEnvelope = getOriginalGridRange();
		return new ImageLayout().setMinX(
				gridEnvelope.getLow(0)).setMinY(
				gridEnvelope.getLow(1)).setTileWidth(
				adapter.getTileSize()).setTileHeight(
				adapter.getTileSize()).setSampleModel(
				adapter.getSampleModel()).setColorModel(
				adapter.getColorModel()).setWidth(
				gridEnvelope.getHigh(0)).setHeight(
				gridEnvelope.getHigh(1));
	}

	@Override
	public double[][] getResolutionLevels()
			throws IOException {
		throw new UnsupportedOperationException(
				"A coverage name must be provided, there is no support for a default coverage");
	}

	@Override
	public double[][] getResolutionLevels(
			final String coverageName )
			throws IOException {
		final Resolution[] resolutions = geowaveDataStore.aggregateStatistics(OverviewStatistics.STATS_TYPE
				.newBuilder()
				.setAuthorizations(
						authorizationSPI.getAuthorizations())
				.dataType(
						coverageName)
				.build());
		if (resolutions == null) {
			LOGGER.warn("Cannot find resolutions for coverage '" + coverageName + "'");
			return null;
		}
		final double[][] retVal = new double[resolutions.length][];
		int i = 0;
		for (final Resolution res : resolutions) {
			retVal[i++] = res.getResolutionPerDimension();
		}
		return retVal;

	}

	private Histogram getHistogram(
			final String coverageName,
			final double resX,
			final double resY )
			throws IOException {
		final Map<Resolution, Histogram> histograms = geowaveDataStore
				.aggregateStatistics(HistogramStatistics.STATS_TYPE.newBuilder().setAuthorizations(
						authorizationSPI.getAuthorizations()).dataType(
						coverageName).build());
		if (histograms != null) {
			return histograms.get(new Resolution(
					new double[] {
						resX,
						resY
					}));
		}
		else {
			LOGGER.warn("Cannot find histogram for coverage '" + coverageName + "'");
		}
		return null;
	}

	/**
	 * @param crs
	 *            CoordinateReference System
	 * @return dimension index of y dir in crs
	 */
	private static int indexOfY(
			final CoordinateReferenceSystem crs ) {
		return indexOf(
				crs,
				UPDirections);
	}

	/**
	 * @param crs
	 *            CoordinateReference System
	 * @return dimension index of X dir in crs
	 */
	private static int indexOfX(
			final CoordinateReferenceSystem crs ) {
		return indexOf(
				crs,
				LEFTDirections);
	}

	private static int indexOf(
			final CoordinateReferenceSystem crs,
			final Set<AxisDirection> direction ) {
		final CoordinateSystem cs = crs.getCoordinateSystem();
		for (int index = 0; index < cs.getDimension(); index++) {
			final CoordinateSystemAxis axis = cs.getAxis(index);
			if (direction.contains(axis.getDirection())) {
				return index;
			}
		}
		return -1;
	}

	private short getAdapterId(
			final String coverageName ) {

		return geowaveInternalAdapterStore.getAdapterId(coverageName);

	}

}
