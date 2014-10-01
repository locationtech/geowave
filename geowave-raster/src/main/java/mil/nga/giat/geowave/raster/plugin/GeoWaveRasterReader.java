package mil.nga.giat.geowave.raster.plugin;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.imageio.ImageReadParam;
import javax.media.jai.Histogram;
import javax.media.jai.ImageLayout;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.raster.RasterDataStore;
import mil.nga.giat.geowave.raster.RasterUtils;
import mil.nga.giat.geowave.raster.Resolution;
import mil.nga.giat.geowave.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.raster.query.IndexOnlySpatialQuery;
import mil.nga.giat.geowave.raster.stats.HistogramStatistics;
import mil.nga.giat.geowave.raster.stats.OverviewStatistics;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.log4j.Logger;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.OverviewPolicy;
import org.geotools.coverage.processing.Operations;
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

/**
 * the reader gets the connection info and returns a grid coverage for every
 * data adapter
 */
public class GeoWaveRasterReader extends
		AbstractGridCoverage2DReader implements
		GridCoverage2DReader
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveRasterReader.class);

	private GeoWaveRasterConfig config;

	private final AdapterStore geowaveAdapterStore;

	private final DataStatisticsStore geowaveStatisticsStore;

	private final RasterDataStore geowaveDataStore;

	private final Index rasterIndex;

	protected final static CoordinateOperationFactory OPERATION_FACTORY = new BufferedCoordinateOperationFactory(
			new Hints(
					Hints.LENIENT_DATUM_SHIFT,
					Boolean.TRUE));
	private static Set<AxisDirection> UPDirections;

	private static Set<AxisDirection> LEFTDirections;
	// class initializer
	{
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
			throws IOException,
			MalformedURLException,
			AccumuloException,
			AccumuloSecurityException {
		super(
				source,
				uHints);
		this.source = source;

		final URL url = GeoWaveGTRasterFormat.getURLFromSource(source);

		if (url == null) {
			throw new MalformedURLException(
					source.toString());
		}

		try {
			config = GeoWaveRasterConfig.readFrom(url);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Cannot read config",
					e);
			throw new IOException(
					e);
		}

		final AccumuloOperations accumuloOperations = new BasicAccumuloOperations(
				config.getZookeeperUrls(),
				config.getAccumuloInstanceId(),
				config.getAccumuloUsername(),
				config.getAccumuloPassword(),
				config.getGeowaveNamespace());
		geowaveAdapterStore = new AccumuloAdapterStore(
				accumuloOperations);

		geowaveStatisticsStore = new AccumuloDataStatisticsStore(
				accumuloOperations);

		geowaveDataStore = new RasterDataStore(
				accumuloOperations);

		rasterIndex = IndexType.SPATIAL_RASTER.createDefaultIndex();
		crs = GeoWaveGTRasterFormat.DEFAULT_CRS;
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
			throws IOException,
			AccumuloException,
			AccumuloSecurityException {
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
			return coverageNames.toArray(new String[] {});
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
		return ((RasterDataAdapter) adapter).getMetadata().keySet().toArray(
				new String[] {});
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
				BoundingBoxDataStatistics.STATS_ID);
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
				BoundingBoxDataStatistics.STATS_ID);
		int width = 0;
		int height = 0;
		// try to use both the bounding box and the overview statistics to
		// determine the width and height at the highest resolution
		if (statistics instanceof BoundingBoxDataStatistics) {
			final BoundingBoxDataStatistics<?> bboxStats = (BoundingBoxDataStatistics<?>) statistics;
			statistics = geowaveStatisticsStore.getDataStatistics(
					new ByteArrayId(
							coverageName),
					OverviewStatistics.STATS_ID);
			if (statistics instanceof OverviewStatistics) {
				final OverviewStatistics overviewStats = (OverviewStatistics) statistics;
				width = (int) Math.ceil(((bboxStats.getMaxX() - bboxStats.getMinX()) / overviewStats.getResolutions()[0].getResolution(0)));
				height = (int) Math.ceil(((bboxStats.getMaxY() - bboxStats.getMinY()) / overviewStats.getResolutions()[0].getResolution(1)));
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
		final GeoWaveRasterReaderState state = new GeoWaveRasterReaderState(
				coverageName);
		final Date start = new Date();
		// /////////////////////////////////////////////////////////////////////
		//
		// Checking params
		//
		// /////////////////////////////////////////////////////////////////////
		Color outputTransparentColor = GeoWaveGTRasterFormat.OUTPUT_TRANSPARENT_COLOR.getDefaultValue();

		Color backgroundColor = AbstractGridFormat.BACKGROUND_COLOR.getDefaultValue();

		Rectangle dim = null;

		if (params != null) {
			for (final GeneralParameterValue generalParameterValue : params) {
				final Parameter<Object> param = (Parameter<Object>) generalParameterValue;

				if (param.getDescriptor().getName().getCode().equals(
						AbstractGridFormat.READ_GRIDGEOMETRY2D.getName().toString())) {
					final GridGeometry2D gg = (GridGeometry2D) param.getValue();
					state.setRequestedEnvelope((GeneralEnvelope) gg.getEnvelope());
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
			}
		}

		// /////////////////////////////////////////////////////////////////////
		//
		// Loading tiles trying to optimize as much as possible
		//
		// /////////////////////////////////////////////////////////////////////
		final GridCoverage2D coverage = loadTiles(
				coverageName,
				backgroundColor,
				outputTransparentColor,
				dim,
				state,
				crs,
				getOriginalEnvelope(coverageName));
		LOGGER.info("Mosaic Reader needs : " + ((new Date()).getTime() - start.getTime()) + " millisecs");

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
										outputTransparentColor),
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
								outputTransparentColor),
						state.getRequestedEnvelope());
			}
		}
		else {
			imageChoice = new Integer(
					0);
		}

		final double[][] resolutionLevels = getResolutionLevels(coverageName);
		Histogram histogram = null;
		if (config.isEqualizeHistogram()) {
			histogram = getHistogram(
					coverageName,
					resolutionLevels[imageChoice.intValue()][0],
					resolutionLevels[imageChoice.intValue()][1]);
		}

		final RasterDataAdapter adapter = (RasterDataAdapter) geowaveAdapterStore.getAdapter(new ByteArrayId(
				coverageName));
		final Iterator<GridCoverage> gridCoverageIt = queryForTiles(
				pixelDimension,
				state.getRequestEnvelopeTransformed(),
				resolutionLevels[imageChoice.intValue()][0],
				resolutionLevels[imageChoice.intValue()][1],
				adapter);
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
				config.getInterpolation(),
				histogram);

		return transformResult(
				result,
				pixelDimension,
				state);
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

	private Iterator<GridCoverage> queryForTiles(
			final Rectangle pixelDimension,
			final GeneralEnvelope requestEnvelope,
			final double levelResX,
			final double levelResY,
			final RasterDataAdapter adapter )
			throws IOException {
		return geowaveDataStore.query(
				adapter,
				rasterIndex,
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

	private GridCoverage2D transformResult(
			final GridCoverage2D coverage,
			final Rectangle pixelDimension,
			final GeoWaveRasterReaderState state ) {
		if (state.getRequestEnvelopeTransformed() == state.getRequestedEnvelope()) {
			return coverage; // nothing to do
		}

		GridCoverage2D result = null;
		LOGGER.info("Image reprojection necessairy");
		result = (GridCoverage2D) Operations.DEFAULT.resample(
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
	private static void transformRequestEnvelope(
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
				OverviewStatistics.STATS_ID);
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
				HistogramStatistics.STATS_ID);
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
