package mil.nga.giat.geowave.adapter.raster.plugin;

import java.awt.Color;
import java.awt.Rectangle;
import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import javax.imageio.ImageReadParam;
import javax.media.jai.Histogram;
import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.adapter.auth.AuthorizationSPI;
import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.adapter.raster.Resolution;
import mil.nga.giat.geowave.adapter.raster.adapter.CompoundHierarchicalIndexStrategyWrapper;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.stats.HistogramStatistics;
import mil.nga.giat.geowave.adapter.raster.stats.OverviewStatistics;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.query.IndexOnlySpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

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

	private AdapterStore geowaveAdapterStore;

	private DataStatisticsStore geowaveStatisticsStore;

	private DataStore geowaveDataStore;

	private IndexStore geowaveIndexStore;

	private AdapterIndexMappingStore geowaveAdapterIndexMappingStore;

	private AuthorizationSPI authorizationSPI;

	protected final static CoordinateOperationFactory OPERATION_FACTORY = new BufferedCoordinateOperationFactory(
			new Hints(
					Hints.LENIENT_DATUM_SHIFT,
					Boolean.TRUE));
	private static Set<AxisDirection> UPDirections;

	private static Set<AxisDirection> LEFTDirections;
	// class initializer
	static {
		LEFTDirections = new HashSet<AxisDirection>();
		LEFTDirections.add(AxisDirection.DISPLAY_LEFT);
		LEFTDirections.add(AxisDirection.EAST);
		LEFTDirections.add(AxisDirection.GEOCENTRIC_X);
		LEFTDirections.add(AxisDirection.COLUMN_POSITIVE);

		UPDirections = new HashSet<AxisDirection>();
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
		crs = GeoWaveGTRasterFormat.DEFAULT_CRS;
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

	@Override
	public Format getFormat() {
		return new GeoWaveGTRasterFormat();
	}

	@Override
	public String[] getGridCoverageNames() {
		try (final CloseableIterator<DataAdapter<?>> it = geowaveAdapterStore.getAdapters()) {
			final List<String> coverageNames = new ArrayList<String>();
			while (it.hasNext()) {
				final DataAdapter<?> adapter = it.next();
				if (adapter instanceof RasterDataAdapter) {
					coverageNames.add(((RasterDataAdapter) adapter).getCoverageName());
				}
			}
			return coverageNames.toArray(new String[coverageNames.size()]);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"unable to close adapter iterator while looking up coverage names",
					e);
		}
		return new String[] {};
	}

	@Override
	public int getGridCoverageCount() {
		try (final CloseableIterator<DataAdapter<?>> it = geowaveAdapterStore.getAdapters()) {
			int coverageCount = 0;
			while (it.hasNext()) {
				final DataAdapter<?> adapter = it.next();
				if (adapter instanceof RasterDataAdapter) {
					coverageCount++;
				}
			}
			return coverageCount;
		}
		catch (final IOException e) {
			LOGGER.warn(
					"unable to close data adapter when looking up coverage count",
					e);
		}
		return 0;
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

		final DataAdapter<?> adapter = geowaveAdapterStore.getAdapter(new ByteArrayId(
				coverageName));
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

		final DataAdapter<?> adapter = geowaveAdapterStore.getAdapter(new ByteArrayId(
				coverageName));
		return ((RasterDataAdapter) adapter).getMetadata().get(
				name);
	}

	@Override
	protected boolean checkName(
			final String coverageName ) {
		Utilities.ensureNonNull(
				"coverageName",
				coverageName);
		final DataAdapter<?> adapter = geowaveAdapterStore.getAdapter(new ByteArrayId(
				coverageName));
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
		final DataStatistics<?> statistics = geowaveStatisticsStore.getDataStatistics(
				new ByteArrayId(
						coverageName),
				BoundingBoxDataStatistics.STATS_TYPE,
				authorizationSPI.getAuthorizations());
		// try to use both the bounding box and the overview statistics to
		// determine the width and height at the highest resolution
		if (statistics instanceof BoundingBoxDataStatistics) {
			final BoundingBoxDataStatistics<?> bboxStats = (BoundingBoxDataStatistics<?>) statistics;
			final GeneralEnvelope env = new GeneralEnvelope(
					new Rectangle2D.Double(
							bboxStats.getMinX(),
							bboxStats.getMinY(),
							bboxStats.getWidth(),
							bboxStats.getHeight()));
			env.setCoordinateReferenceSystem(GeoWaveGTRasterFormat.DEFAULT_CRS);
			return env;
		}
		return null;
	}

	@Override
	public CoordinateReferenceSystem getCoordinateReferenceSystem() {
		return GeoWaveGTRasterFormat.DEFAULT_CRS;
	}

	@Override
	public CoordinateReferenceSystem getCoordinateReferenceSystem(
			final String coverageName ) {
		return GeoWaveGTRasterFormat.DEFAULT_CRS;
	}

	@Override
	public GridEnvelope getOriginalGridRange() {
		throw new UnsupportedOperationException(
				"A coverage name must be provided, there is no support for a default coverage");
	}

	@Override
	public GridEnvelope getOriginalGridRange(
			final String coverageName ) {
		DataStatistics<?> statistics = geowaveStatisticsStore.getDataStatistics(
				new ByteArrayId(
						coverageName),
				BoundingBoxDataStatistics.STATS_TYPE,
				authorizationSPI.getAuthorizations());
		int width = 0;
		int height = 0;
		// try to use both the bounding box and the overview statistics to
		// determine the width and height at the highest resolution
		if (statistics instanceof BoundingBoxDataStatistics) {
			final BoundingBoxDataStatistics<?> bboxStats = (BoundingBoxDataStatistics<?>) statistics;
			statistics = geowaveStatisticsStore.getDataStatistics(
					new ByteArrayId(
							coverageName),
					OverviewStatistics.STATS_TYPE,
					authorizationSPI.getAuthorizations());
			if (statistics instanceof OverviewStatistics) {
				final OverviewStatistics overviewStats = (OverviewStatistics) statistics;
				width = (int) Math
						.ceil(((bboxStats.getMaxX() - bboxStats.getMinX()) / overviewStats.getResolutions()[0]
								.getResolution(0)));
				height = (int) Math
						.ceil(((bboxStats.getMaxY() - bboxStats.getMinY()) / overviewStats.getResolutions()[0]
								.getResolution(1)));
			}
		}

		return new GridEnvelope2D(
				0,
				0,
				width,
				height);
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
				crs,
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
		if (!state.getRequestEnvelopeTransformed().intersects(
				originalEnvelope,
				true)) {
			LOGGER.warn("The requested envelope does not intersect the envelope of this mosaic");
			LOGGER.warn(state.getRequestEnvelopeTransformed().toString());
			LOGGER.warn(originalEnvelope.toString());

			return null;
		}

		final ImageReadParam readP = new ImageReadParam();
		final Integer imageChoice;

		final RasterDataAdapter adapter = (RasterDataAdapter) geowaveAdapterStore.getAdapter(new ByteArrayId(
				coverageName));
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
							state.getRequestEnvelopeTransformed(),
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

		boolean scaleTo8BitSet = config.isScaleTo8BitSet();
		if (scaleTo8BitSet) {
			scaleTo8Bit = config.isScaleTo8Bit();
		}

		try (final CloseableIterator<GridCoverage> gridCoverageIt = queryForTiles(
				pixelDimension,
				state.getRequestEnvelopeTransformed(),
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
					state.getRequestEnvelopeTransformed(),
					resolutionLevels[imageChoice.intValue()][0],
					resolutionLevels[imageChoice.intValue()][1],
					adapter.getNoDataValuesPerBand(),
					state.isXAxisSwitch(),
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
		if (resLevels.length == 0) {
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
		return queryForTiles(
				adapter,
				new IndexOnlySpatialQuery(
						new GeometryFactory().toGeometry(new Envelope(
								requestEnvelope.getMinimum(0),
								requestEnvelope.getMaximum(0),
								requestEnvelope.getMinimum(1),
								requestEnvelope.getMaximum(1)))),
				new double[] {
					levelResX * adapter.getTileSize(),
					levelResY * adapter.getTileSize()
				});
	}

	private CloseableIterator<GridCoverage> queryForTiles(
			final RasterDataAdapter adapter,
			final Query query,
			final double[] targetResolutionPerDimension ) {
		final AdapterToIndexMapping adapterIndexMapping = geowaveAdapterIndexMappingStore.getIndicesForAdapter(adapter
				.getAdapterId());
		final PrimaryIndex[] indices = adapterIndexMapping.getIndices(geowaveIndexStore);
		// just work on the first spatial only index that contains this adapter
		// ID
		// TODO consider the best strategy for handling temporal queries here
		for (final PrimaryIndex rasterIndex : indices) {
			if (SpatialDimensionalityTypeProvider.isSpatial(rasterIndex)) {
				// determine the correct tier to query for the given resolution
				final HierarchicalNumericIndexStrategy strategy = CompoundHierarchicalIndexStrategyWrapper
						.findHierarchicalStrategy(rasterIndex.getIndexStrategy());
				if (strategy != null) {
					final TreeMap<Double, SubStrategy> sortedStrategies = new TreeMap<Double, SubStrategy>();
					SubStrategy targetIndexStrategy = null;
					for (final SubStrategy subStrategy : strategy.getSubStrategies()) {
						final double[] idRangePerDimension = subStrategy
								.getIndexStrategy()
								.getHighestPrecisionIdRangePerDimension();
						double rangeSum = 0;
						for (final double range : idRangePerDimension) {
							rangeSum += range;
						}
						// sort by the sum of the range in each dimension
						sortedStrategies.put(
								rangeSum,
								subStrategy);
					}
					for (final SubStrategy subStrategy : sortedStrategies.descendingMap().values()) {
						final double[] highestPrecisionIdRangePerDimension = subStrategy
								.getIndexStrategy()
								.getHighestPrecisionIdRangePerDimension();
						// if the id range is less than or equal to the target
						// resolution in each dimension, use this substrategy
						boolean withinTargetResolution = true;
						for (int d = 0; d < highestPrecisionIdRangePerDimension.length; d++) {
							if (highestPrecisionIdRangePerDimension[d] > targetResolutionPerDimension[d]) {
								withinTargetResolution = false;
								break;
							}
						}
						if (withinTargetResolution) {
							targetIndexStrategy = subStrategy;
							break;
						}
					}
					if (targetIndexStrategy == null) {
						// if there is not a substrategy that is within the
						// target
						// resolution, use the first substrategy (the lowest
						// range
						// per
						// dimension, which is the highest precision)
						targetIndexStrategy = sortedStrategies.firstEntry().getValue();
					}
					return geowaveDataStore.query(
							new QueryOptions(
									adapter,
									new CustomIdIndex(
											// replace the index strategy with a
											// single
											// substrategy that fits the target
											// resolution
											targetIndexStrategy.getIndexStrategy(),
											rasterIndex.getIndexModel(),
											rasterIndex.getId()), // make sure
																	// the
																	// index ID
																	// is
																	// the
									authorizationSPI.getAuthorizations()),
							// same as the orginal so that we
							// are querying the correct table
							query);
				}
				else {
					return geowaveDataStore.query(
							new QueryOptions(
									adapter,
									rasterIndex,
									authorizationSPI.getAuthorizations()),
							query);
				}
			}
		}
		return new Wrapper(
				Collections.emptyIterator());
	}

	private GridCoverage2D transformResult(
			final GridCoverage2D coverage,
			final Rectangle pixelDimension,
			final GeoWaveRasterReaderState state ) {
		if (state.getRequestEnvelopeTransformed() == state.getRequestedEnvelope()) {
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
			state.setRequestEnvelopeTransformed(state.getRequestedEnvelope());

			return; // and finish
		}

		try {
			/** Buffered factory for coordinate operations. */

			// transforming the envelope back to the dataset crs in
			final MathTransform transform = OPERATION_FACTORY.createOperation(
					state.getRequestedEnvelope().getCoordinateReferenceSystem(),
					crs).getMathTransform();

			if (transform.isIdentity()) { // Identity Transform ?
				state.setRequestEnvelopeTransformed(state.getRequestedEnvelope());
				return; // and finish
			}

			state.setRequestEnvelopeTransformed(CRS.transform(
					transform,
					state.getRequestedEnvelope()));
			state.getRequestEnvelopeTransformed().setCoordinateReferenceSystem(
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
				state.setXAxisSwitch(true);
				final Rectangle2D tmp = new Rectangle2D.Double(
						state.getRequestEnvelopeTransformed().getMinimum(
								1),
						state.getRequestEnvelopeTransformed().getMinimum(
								0),
						state.getRequestEnvelopeTransformed().getSpan(
								1),
						state.getRequestEnvelopeTransformed().getSpan(
								0));
				state.setRequestEnvelopeTransformed(new GeneralEnvelope(
						tmp));
				state.getRequestEnvelopeTransformed().setCoordinateReferenceSystem(
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

		final RasterDataAdapter adapter = (RasterDataAdapter) geowaveAdapterStore.getAdapter(new ByteArrayId(
				coverageName));
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
		final DataStatistics<?> stats = geowaveStatisticsStore.getDataStatistics(
				new ByteArrayId(
						coverageName),
				OverviewStatistics.STATS_TYPE,
				authorizationSPI.getAuthorizations());
		if ((stats != null) && (stats instanceof OverviewStatistics)) {
			final Resolution[] resolutions = ((OverviewStatistics) stats).getResolutions();
			final double[][] retVal = new double[resolutions.length][];
			int i = 0;
			for (final Resolution res : resolutions) {
				retVal[i++] = res.getResolutionPerDimension();
			}
			return retVal;
		}
		return new double[][] {};
	}

	private Histogram getHistogram(
			final String coverageName,
			final double resX,
			final double resY )
			throws IOException {
		final DataStatistics<?> stats = geowaveStatisticsStore.getDataStatistics(
				new ByteArrayId(
						coverageName),
				HistogramStatistics.STATS_TYPE,
				authorizationSPI.getAuthorizations());
		if ((stats != null) && (stats instanceof HistogramStatistics)) {
			return ((HistogramStatistics) stats).getHistogram(new Resolution(
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

}
