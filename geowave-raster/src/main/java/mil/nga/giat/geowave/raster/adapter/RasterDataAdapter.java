package mil.nga.giat.geowave.raster.adapter;

import java.awt.Color;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.measure.unit.Unit;
import javax.media.jai.Interpolation;
import javax.media.jai.PlanarImage;
import javax.media.jai.remote.SerializableState;
import javax.media.jai.remote.SerializerFactory;

import mil.nga.giat.geowave.accumulo.AttachedIteratorDataAdapter;
import mil.nga.giat.geowave.accumulo.IteratorConfig;
import mil.nga.giat.geowave.accumulo.util.IteratorWrapper;
import mil.nga.giat.geowave.accumulo.util.IteratorWrapper.Converter;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.raster.FitToIndexGridCoverage;
import mil.nga.giat.geowave.raster.FitToIndexPersistenceEncoding;
import mil.nga.giat.geowave.raster.RasterUtils;
import mil.nga.giat.geowave.raster.Resolution;
import mil.nga.giat.geowave.raster.adapter.merge.RasterTileCombiner;
import mil.nga.giat.geowave.raster.adapter.merge.RasterTileCombinerConfig;
import mil.nga.giat.geowave.raster.adapter.merge.RasterTileCombinerHelper;
import mil.nga.giat.geowave.raster.adapter.merge.RasterTileMergeStrategy;
import mil.nga.giat.geowave.raster.adapter.merge.RasterTileVisibilityCombiner;
import mil.nga.giat.geowave.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.raster.plugin.GeoWaveGTRasterFormat;
import mil.nga.giat.geowave.raster.stats.HistogramConfig;
import mil.nga.giat.geowave.raster.stats.HistogramStatistics;
import mil.nga.giat.geowave.raster.stats.OverviewStatistics;
import mil.nga.giat.geowave.raster.stats.RasterBoundingBoxStatistics;
import mil.nga.giat.geowave.raster.stats.RasterFootprintStatistics;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.store.adapter.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsVisibilityHandler;
import mil.nga.giat.geowave.store.adapter.statistics.FieldIdStatisticVisibility;
import mil.nga.giat.geowave.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.store.data.PersistentDataset;
import mil.nga.giat.geowave.store.data.PersistentValue;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldWriter;
import mil.nga.giat.geowave.store.index.CommonIndexModel;
import mil.nga.giat.geowave.store.index.CommonIndexValue;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.log4j.Logger;
import org.geotools.coverage.Category;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.TypeMap;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.processing.Operations;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.GeometryClipper;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.renderer.lite.RendererUtilities;
import org.geotools.resources.coverage.CoverageUtilities;
import org.geotools.resources.i18n.Vocabulary;
import org.geotools.resources.i18n.VocabularyKeys;
import org.geotools.util.NumberRange;
import org.geotools.util.SimpleInternationalString;
import org.opengis.coverage.ColorInterpretation;
import org.opengis.coverage.SampleDimension;
import org.opengis.coverage.SampleDimensionType;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.coverage.grid.GridEnvelope;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform1D;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.InternationalString;

import com.vividsolutions.jts.geom.Geometry;

public class RasterDataAdapter implements
		StatisticalDataAdapter<GridCoverage>,
		IndexDependentDataAdapter<GridCoverage>,
		AttachedIteratorDataAdapter<GridCoverage>
{ // these priorities are fairly arbitrary at the moment
	private static final int RASTER_TILE_COMBINER_PRIORITY = 4;
	private static final int RASTER_TILE_VISIBILITY_COMBINER_PRIORITY = 6;
	private final static Logger LOGGER = Logger.getLogger(RasterDataAdapter.class);

	private final static ByteArrayId DATA_FIELD_ID = new ByteArrayId(
			"image");
	private final static int DEFAULT_TILE_SIZE = 256;
	private final static boolean DEFAULT_BUILD_PYRAMID = false;

	/**
	 * A transparent color for missing data.
	 */
	private static final Color TRANSPARENT = new Color(
			0,
			0,
			0,
			0);
	private String coverageName;
	private int tileSize;
	private SampleModel sampleModel;
	private ColorModel colorModel;
	private Map<String, String> metadata;
	private HistogramConfig histogramConfig;
	private double[][] noDataValuesPerBand;
	private double[] backgroundValuesPerBand;
	private boolean buildPyramid;
	private ByteArrayId[] supportedStatsIds;
	private DataStatisticsVisibilityHandler<GridCoverage> visibilityHandler;
	private RasterTileMergeStrategy<?> mergeStrategy;

	protected RasterDataAdapter() {}

	public RasterDataAdapter(
			final String coverageName,
			final Map<String, String> metadata,
			final GridCoverage2D originalGridCoverage ) {
		this(
				coverageName,
				metadata,
				originalGridCoverage,
				DEFAULT_TILE_SIZE,
				DEFAULT_BUILD_PYRAMID,
				new NoDataMergeStrategy(
						new ByteArrayId(
								coverageName),
						originalGridCoverage.getRenderedImage().getSampleModel().createCompatibleSampleModel(
								DEFAULT_TILE_SIZE,
								DEFAULT_TILE_SIZE)));
	}

	public RasterDataAdapter(
			final String coverageName,
			final Map<String, String> metadata,
			final GridCoverage2D originalGridCoverage,
			final int tileSize,
			final boolean buildPyramid,
			final RasterTileMergeStrategy<?> mergeStrategy ) {
		final RenderedImage img = originalGridCoverage.getRenderedImage();
		sampleModel = img.getSampleModel();
		colorModel = img.getColorModel();
		this.metadata = metadata;
		this.coverageName = coverageName;
		this.tileSize = tileSize;
		histogramConfig = new HistogramConfig(
				sampleModel);
		noDataValuesPerBand = new double[originalGridCoverage.getNumSampleDimensions()][];
		for (int d = 0; d < noDataValuesPerBand.length; d++) {
			noDataValuesPerBand[d] = originalGridCoverage.getSampleDimension(
					d).getNoDataValues();
		}
		backgroundValuesPerBand = CoverageUtilities.getBackgroundValues(originalGridCoverage);
		this.buildPyramid = buildPyramid;
		this.mergeStrategy = mergeStrategy;
		init();
	}

	public RasterDataAdapter(
			final String coverageName,
			final SampleModel sampleModel,
			final ColorModel colorModel,
			final Map<String, String> metadata,
			final int tileSize,
			final double[][] noDataValuesPerBand,
			final double[] backgroundValuesPerBand,
			final boolean buildPyramid ) {
		this(
				coverageName,
				sampleModel,
				colorModel,
				metadata,
				tileSize,
				noDataValuesPerBand,
				backgroundValuesPerBand,
				new HistogramConfig(
						sampleModel),
				buildPyramid,
				new NoDataMergeStrategy(
						new ByteArrayId(
								coverageName),
						sampleModel.createCompatibleSampleModel(
								tileSize,
								tileSize)));
	}

	public RasterDataAdapter(
			final String coverageName,
			final SampleModel sampleModel,
			final ColorModel colorModel,
			final Map<String, String> metadata,
			final int tileSize,
			final double[][] noDataValuesPerBand,
			final double[] backgroundValuesPerBand,
			final HistogramConfig histogramConfig,
			final boolean buildPyramid,
			final RasterTileMergeStrategy<?> mergeStrategy ) {
		this.coverageName = coverageName;
		this.tileSize = tileSize;
		this.sampleModel = sampleModel;
		this.colorModel = colorModel;
		this.metadata = metadata;
		this.noDataValuesPerBand = noDataValuesPerBand;
		this.backgroundValuesPerBand = backgroundValuesPerBand;
		// a null histogram config will result in histogram statistics not being
		// accumulated
		this.histogramConfig = histogramConfig;
		this.buildPyramid = buildPyramid;
		this.mergeStrategy = mergeStrategy;
		init();
	}

	private void init() {

		int supportedStatsLength = 3;

		if (histogramConfig != null) {
			supportedStatsLength++;
		}

		supportedStatsIds = new ByteArrayId[supportedStatsLength];
		supportedStatsIds[0] = OverviewStatistics.STATS_ID;
		supportedStatsIds[1] = BoundingBoxDataStatistics.STATS_ID;
		supportedStatsIds[2] = RasterFootprintStatistics.STATS_ID;

		if (histogramConfig != null) {
			supportedStatsIds[3] = HistogramStatistics.STATS_ID;
		}
		visibilityHandler = new FieldIdStatisticVisibility<GridCoverage>(
				DATA_FIELD_ID);
	}

	@Override
	public Iterator<GridCoverage> convertToIndex(
			final Index index,
			final GridCoverage gridCoverage ) {
		if (index.getIndexStrategy() instanceof HierarchicalNumericIndexStrategy) {
			final CoordinateReferenceSystem sourceCrs = gridCoverage.getCoordinateReferenceSystem();

			final Envelope sampleEnvelope = gridCoverage.getEnvelope();

			final ReferencedEnvelope sampleReferencedEnvelope = new ReferencedEnvelope(
					new com.vividsolutions.jts.geom.Envelope(
							sampleEnvelope.getMinimum(0),
							sampleEnvelope.getMaximum(0),
							sampleEnvelope.getMinimum(1),
							sampleEnvelope.getMaximum(1)),
					gridCoverage.getCoordinateReferenceSystem());

			final ReferencedEnvelope projectedReferenceEnvelope;
			if (GeoWaveGTRasterFormat.DEFAULT_CRS.equals(sourceCrs)) {
				projectedReferenceEnvelope = sampleReferencedEnvelope;
			}
			else {
				try {
					projectedReferenceEnvelope = sampleReferencedEnvelope.transform(
							GeoWaveGTRasterFormat.DEFAULT_CRS,
							true);
				}
				catch (TransformException | FactoryException e) {
					LOGGER.warn(
							"Unable to transform envelope of grid coverage to EPSG:4326",
							e);
					return null;
				}
			}

			final MultiDimensionalNumericData bounds = GeometryUtils.basicConstraintsFromEnvelope(
					projectedReferenceEnvelope).getIndexConstraints(
					index.getIndexStrategy());
			final GridEnvelope gridEnvelope = gridCoverage.getGridGeometry().getGridRange();
			final double[] tileRangePerDimension = new double[bounds.getDimensionCount()];
			final double[] maxValuesPerDimension = bounds.getMaxValuesPerDimension();
			final double[] minValuesPerDimension = bounds.getMinValuesPerDimension();
			double maxSpan = -Double.MAX_VALUE;
			for (int d = 0; d < tileRangePerDimension.length; d++) {
				tileRangePerDimension[d] = ((maxValuesPerDimension[d] - minValuesPerDimension[d]) * tileSize) / gridEnvelope.getSpan(d);

				maxSpan = Math.max(
						gridEnvelope.getSpan(d),
						maxSpan);
			}
			final HierarchicalNumericIndexStrategy imagePyramid = (HierarchicalNumericIndexStrategy) index.getIndexStrategy();
			final TreeMap<Double, SubStrategy> substrategyMap = new TreeMap<Double, SubStrategy>();
			for (final SubStrategy pyramidLevel : imagePyramid.getSubStrategies()) {
				final double[] idRangePerDimension = pyramidLevel.getIndexStrategy().getHighestPrecisionIdRangePerDimension();
				// to create a pyramid, ingest into each substrategy that is
				// lower resolution than the sample set in at least one
				// dimension and the one substrategy that is at least the same
				// resolution or higher resolution to retain the original
				// resolution as well as possible
				double maxSubstrategyResToSampleSetRes = -Double.MAX_VALUE;

				for (int d = 0; d < tileRangePerDimension.length; d++) {
					final double substrategyResToSampleSetRes = idRangePerDimension[d] / tileRangePerDimension[d];
					maxSubstrategyResToSampleSetRes = Math.max(
							maxSubstrategyResToSampleSetRes,
							substrategyResToSampleSetRes);
				}
				substrategyMap.put(
						maxSubstrategyResToSampleSetRes,
						pyramidLevel);
			}
			final SortedMap<Double, SubStrategy> map = substrategyMap.tailMap(
					1.0,
					false).headMap(
					maxSpan / tileSize,
					true);
			// all entries will be greater than 1 (lower resolution pyramid
			// levels)
			// also try to find the one entry that is closest to 1.0 without
			// going over (this will be the full resolution level)
			final Entry<Double, SubStrategy> fullResEntry = substrategyMap.floorEntry(1.0);
			final List<SubStrategy> pyramidLevels = new ArrayList<SubStrategy>();
			if (fullResEntry != null) {
				pyramidLevels.add(fullResEntry.getValue());
			}
			if (buildPyramid) {
				pyramidLevels.addAll(map.values());
			}
			if (pyramidLevels.isEmpty()) {
				// this case shouldn't occur theoretically, but just in case,
				// make sure the substrategy closest to 1.0 is used
				final Entry<Double, SubStrategy> bestEntry = substrategyMap.higherEntry(1.0);
				pyramidLevels.add(bestEntry.getValue());
			}
			return new IteratorWrapper<SubStrategy, GridCoverage>(
					pyramidLevels.iterator(),
					new MosaicPerPyramidLevelBuilder(
							bounds,
							gridCoverage,
							tileSize,
							backgroundValuesPerBand,
							RasterUtils.getFootprint(
									projectedReferenceEnvelope,
									gridCoverage)));
		}
		return null;
	}

	private static class MosaicPerPyramidLevelBuilder implements
			Converter<SubStrategy, GridCoverage>
	{
		private final MultiDimensionalNumericData originalBounds;
		private final GridCoverage originalData;
		private final int tileSize;
		private final double[] backgroundValuesPerBand;
		private final Geometry footprint;

		public MosaicPerPyramidLevelBuilder(
				final MultiDimensionalNumericData originalBounds,
				final GridCoverage originalData,
				final int tileSize,
				final double[] backgroundValuesPerBand,
				final Geometry footprint ) {
			this.originalBounds = originalBounds;
			this.originalData = originalData;
			this.tileSize = tileSize;
			this.backgroundValuesPerBand = backgroundValuesPerBand;
			this.footprint = footprint;
		}

		@Override
		public Iterator<GridCoverage> convert(
				final SubStrategy pyramidLevel ) {
			final Iterator<ByteArrayId> insertionIds = pyramidLevel.getIndexStrategy().getInsertionIds(
					originalBounds).iterator();
			return new Iterator<GridCoverage>() {

				@Override
				public boolean hasNext() {
					return insertionIds.hasNext();
				}

				@Override
				public GridCoverage next() {
					final ByteArrayId insertionId = insertionIds.next();
					if (insertionId == null) {
						return null;
					}
					final MultiDimensionalNumericData rangePerDimension = pyramidLevel.getIndexStrategy().getRangeForId(
							insertionId);
					final NumericDimensionDefinition[] dimensions = pyramidLevel.getIndexStrategy().getOrderedDimensionDefinitions();
					int longitudeIndex = 0, latitudeIndex = 1;
					final double[] minDP = new double[2];
					final double[] maxDP = new double[2];
					for (int d = 0; d < dimensions.length; d++) {
						if (dimensions[d] instanceof LatitudeDefinition) {
							latitudeIndex = d;
							minDP[1] = originalBounds.getMinValuesPerDimension()[d];
							maxDP[1] = originalBounds.getMaxValuesPerDimension()[d];
						}
						else if (dimensions[d] instanceof LongitudeDefinition) {
							longitudeIndex = d;
							minDP[0] = originalBounds.getMinValuesPerDimension()[d];
							maxDP[0] = originalBounds.getMaxValuesPerDimension()[d];
						}
					}

					final Envelope originalEnvelope = new GeneralEnvelope(
							minDP,
							maxDP);
					final double[] minsPerDimension = rangePerDimension.getMinValuesPerDimension();
					final double[] maxesPerDimension = rangePerDimension.getMaxValuesPerDimension();
					final ReferencedEnvelope mapExtent = new ReferencedEnvelope(
							minsPerDimension[longitudeIndex],
							maxesPerDimension[longitudeIndex],
							minsPerDimension[latitudeIndex],
							maxesPerDimension[latitudeIndex],
							GeoWaveGTRasterFormat.DEFAULT_CRS);
					final AffineTransform worldToScreenTransform = RendererUtilities.worldToScreenTransform(
							mapExtent,
							new Rectangle(
									tileSize,
									tileSize));
					GridGeometry2D insertionIdGeometry;
					try {
						final AffineTransform2D gridToCRS = new AffineTransform2D(
								worldToScreenTransform.createInverse());
						insertionIdGeometry = new GridGeometry2D(
								new GridEnvelope2D(
										new Rectangle(
												tileSize,
												tileSize)),
								PixelInCell.CELL_CORNER,
								gridToCRS,
								GeoWaveGTRasterFormat.DEFAULT_CRS,
								null);

						final double[] tileRes = pyramidLevel.getIndexStrategy().getHighestPrecisionIdRangePerDimension();
						final double[] pixelRes = new double[tileRes.length];
						for (int d = 0; d < tileRes.length; d++) {
							pixelRes[d] = tileRes[d] / tileSize;
						}
						Geometry footprintWithinTileWorldGeom = null;
						Geometry footprintWithinTileScreenGeom = null;
						try {
							final Geometry wholeFootprintScreenGeom = JTS.transform(
									footprint,
									new AffineTransform2D(
											worldToScreenTransform));
							final GeometryClipper tileClipper = new GeometryClipper(
									new com.vividsolutions.jts.geom.Envelope(
											0,
											tileSize,
											0,
											tileSize));
							footprintWithinTileScreenGeom = tileClipper.clip(
									wholeFootprintScreenGeom,
									true);
							footprintWithinTileWorldGeom = JTS.transform(
									footprintWithinTileScreenGeom,
									gridToCRS);
						}
						catch (final TransformException e) {
							LOGGER.warn(
									"Unable to calculate geometry of footprint for tile",
									e);
						}

						return new FitToIndexGridCoverage(
								(GridCoverage) Operations.DEFAULT.resample(
										originalData,
										GeoWaveGTRasterFormat.DEFAULT_CRS,
										insertionIdGeometry,
										Interpolation.getInstance(Interpolation.INTERP_BICUBIC),
										backgroundValuesPerBand),
								insertionId,
								new Resolution(
										pixelRes),
								originalEnvelope,
								footprintWithinTileWorldGeom,
								footprintWithinTileScreenGeom);
					}
					catch (IllegalArgumentException | NoninvertibleTransformException e) {
						LOGGER.warn(
								"Unable to calculate transformation for grid coordinates on write",
								e);
					}
					return null;
				}

				@Override
				public void remove() {
					insertionIds.remove();
				}
			};
		}
	}

	@Override
	public ByteArrayId getAdapterId() {
		return new ByteArrayId(
				getCoverageName());
	}

	@Override
	public boolean isSupported(
			final GridCoverage entry ) {
		if (!getSampleModel().equals(
				entry.getRenderedImage().getSampleModel())) {
			return false;
		}
		if (!getColorModel().equals(
				entry.getRenderedImage().getColorModel())) {
			return false;
		}
		return true;
	}

	@Override
	public ByteArrayId getDataId(
			final GridCoverage entry ) {
		return new ByteArrayId(
				new byte[] {});
	}

	@Override
	public GridCoverage decode(
			final IndexedAdapterPersistenceEncoding data,
			final Index index ) {
		final Object rasterTile = data.getAdapterExtendedData().getValue(
				DATA_FIELD_ID);
		if ((rasterTile == null) || !(rasterTile instanceof RasterTile)) {
			return null;
		}
		final MultiDimensionalNumericData indexRange = index.getIndexStrategy().getRangeForId(
				data.getIndexInsertionId());
		final NumericDimensionDefinition[] orderedDimensions = index.getIndexStrategy().getOrderedDimensionDefinitions();

		final double[] minsPerDimension = indexRange.getMinValuesPerDimension();
		final double[] maxesPerDimension = indexRange.getMaxValuesPerDimension();
		Double minX = null;
		Double maxX = null;
		Double minY = null;
		Double maxY = null;
		for (int d = 0; d < orderedDimensions.length; d++) {
			if (orderedDimensions[d] instanceof LongitudeDefinition) {
				minX = minsPerDimension[d];
				maxX = maxesPerDimension[d];
			}
			else if (orderedDimensions[d] instanceof LatitudeDefinition) {
				minY = minsPerDimension[d];
				maxY = maxesPerDimension[d];
			}
		}
		if ((minX == null) || (minY == null) || (maxX == null) || (maxY == null)) {
			return null;
		}
		final ReferencedEnvelope mapExtent = new ReferencedEnvelope(
				minsPerDimension[0],
				maxesPerDimension[0],
				minsPerDimension[1],
				maxesPerDimension[1],
				GeoWaveGTRasterFormat.DEFAULT_CRS);
		try {
			return prepareCoverage(
					((RasterTile) rasterTile).getDataBuffer(),
					mapExtent);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to build grid coverage from adapter encoded data",
					e);
		}
		return null;
	}

	/**
	 * This method is responsible for creating a coverage from the supplied
	 * {@link RenderedImage}.
	 *
	 * @param image
	 * @return
	 * @throws IOException
	 */
	private GridCoverage2D prepareCoverage(
			final DataBuffer dataBuffer,
			final ReferencedEnvelope mapExtent )
			throws IOException {
		final SampleModel sm = sampleModel.createCompatibleSampleModel(
				tileSize,
				tileSize);
		WritableRaster raster = Raster.createWritableRaster(
				sm,
				null);

		final boolean alphaPremultiplied = colorModel.isAlphaPremultiplied();

		raster = Raster.createWritableRaster(
				sm,
				dataBuffer,
				null);
		final BufferedImage image = new BufferedImage(
				colorModel,
				raster,
				alphaPremultiplied,
				null);
		// creating bands
		final ColorModel cm = image.getColorModel();
		final int numBands = sm.getNumBands();
		final GridSampleDimension[] bands = new GridSampleDimension[numBands];
		final Set<String> bandNames = new HashSet<String>();
		// setting bands names.
		for (int i = 0; i < numBands; i++) {
			ColorInterpretation colorInterpretation = null;
			String bandName = null;
			if (cm != null) {
				// === color interpretation
				colorInterpretation = TypeMap.getColorInterpretation(
						cm,
						i);
				if (colorInterpretation == null) {
					throw new IOException(
							"Unrecognized sample dimension type");
				}

				bandName = colorInterpretation.name();
				if ((colorInterpretation == ColorInterpretation.UNDEFINED) || bandNames.contains(bandName)) {// make
																												// sure
																												// we
																												// create
																												// no
																												// duplicate
																												// band
																												// names
					bandName = "Band" + (i + 1);
				}
			}
			else { // no color model
				bandName = "Band" + (i + 1);
				colorInterpretation = ColorInterpretation.UNDEFINED;
			}

			// sample dimension type
			final SampleDimensionType st = TypeMap.getSampleDimensionType(
					sm,
					i);

			// set some no data values, as well as Min and Max values
			double noData;
			double min = -Double.MAX_VALUE, max = Double.MAX_VALUE;
			if (st.compareTo(SampleDimensionType.REAL_32BITS) == 0) {
				noData = Float.NaN;
			}
			else if (st.compareTo(SampleDimensionType.REAL_64BITS) == 0) {
				noData = Double.NaN;
			}
			else if (st.compareTo(SampleDimensionType.SIGNED_16BITS) == 0) {
				noData = Short.MIN_VALUE;
				min = Short.MIN_VALUE;
				max = Short.MAX_VALUE;
			}
			else if (st.compareTo(SampleDimensionType.SIGNED_32BITS) == 0) {
				noData = Integer.MIN_VALUE;

				min = Integer.MIN_VALUE;
				max = Integer.MAX_VALUE;
			}
			else if (st.compareTo(SampleDimensionType.SIGNED_8BITS) == 0) {
				noData = -128;
				min = -128;
				max = 127;
			}
			else {
				// unsigned
				noData = 0;
				min = 0;

				// compute max
				if (st.compareTo(SampleDimensionType.UNSIGNED_1BIT) == 0) {
					max = 1;
				}
				else if (st.compareTo(SampleDimensionType.UNSIGNED_2BITS) == 0) {
					max = 3;
				}
				else if (st.compareTo(SampleDimensionType.UNSIGNED_4BITS) == 0) {
					max = 7;
				}
				else if (st.compareTo(SampleDimensionType.UNSIGNED_8BITS) == 0) {
					max = 255;
				}
				else if (st.compareTo(SampleDimensionType.UNSIGNED_16BITS) == 0) {
					max = 65535;
				}
				else if (st.compareTo(SampleDimensionType.UNSIGNED_32BITS) == 0) {
					max = Math.pow(
							2,
							32) - 1;
				}

			}

			if ((noDataValuesPerBand != null) && (noDataValuesPerBand[i] != null) && (noDataValuesPerBand[i].length > 0)) {
				// just take the first value, even if there are multiple
				noData = noDataValuesPerBand[i][0];
			}
			bands[i] = new SimplifiedGridSampleDimension(
					bandName,
					st,
					colorInterpretation,
					noData,
					min,
					max,
					1, // no scale
					0, // no offset
					null).geophysics(true);
		}
		final AffineTransform worldToScreenTransform = RendererUtilities.worldToScreenTransform(
				mapExtent,
				new Rectangle(
						tileSize,
						tileSize));
		try {
			final AffineTransform2D gridToCRS = new AffineTransform2D(
					worldToScreenTransform.createInverse());
			final GridCoverageFactory gcf = CoverageFactoryFinder.getGridCoverageFactory(null);
			return gcf.create(
					coverageName,
					image,
					new GridGeometry2D(
							new GridEnvelope2D(
									PlanarImage.wrapRenderedImage(
											image).getBounds()),
							PixelInCell.CELL_CORNER,
							gridToCRS,
							GeoWaveGTRasterFormat.DEFAULT_CRS,
							null),
					bands,
					null,
					null);

		}
		catch (IllegalArgumentException | NoninvertibleTransformException e) {
			LOGGER.warn(
					"Unable to calculate transformation for grid coordinates on read",
					e);
		}
		return null;
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final GridCoverage entry,
			final CommonIndexModel indexModel ) {
		final SampleModel sm = sampleModel.createCompatibleSampleModel(
				tileSize,
				tileSize);
		final PersistentDataset<Object> adapterExtendedData = new PersistentDataset<Object>();
		adapterExtendedData.addValue(new PersistentValue<Object>(
				DATA_FIELD_ID,
				new RasterTile(
						entry.getRenderedImage().copyData(
								new InternalWritableRaster(
										sm,
										new Point())).getDataBuffer(),
						mergeStrategy.getMetadata(
								entry,
								this))));
		final AdapterPersistenceEncoding encoding;
		if (entry instanceof FitToIndexGridCoverage) {
			encoding = new FitToIndexPersistenceEncoding(
					getAdapterId(),
					new ByteArrayId(
							new byte[] {}),
					new PersistentDataset<CommonIndexValue>(),
					adapterExtendedData,
					((FitToIndexGridCoverage) entry).getIndexId());
		}
		else {
			// this shouldn't happen
			LOGGER.warn("Grid coverage is not fit to the index");
			encoding = new AdapterPersistenceEncoding(
					getAdapterId(),
					new ByteArrayId(
							new byte[] {}),
					new PersistentDataset<CommonIndexValue>(),
					adapterExtendedData);
		}
		return encoding;
	}

	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		if (DATA_FIELD_ID.equals(fieldId)) {
			return (FieldReader) new RasterTileReader();
		}
		return null;
	}

	@Override
	public byte[] toBinary() {
		final byte[] coverageNameBytes = StringUtils.stringToBinary(coverageName);
		final SerializableState serializableSampleModel = SerializerFactory.getState(sampleModel);
		byte[] sampleModelBinary = new byte[0];
		try {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final ObjectOutputStream oos = new ObjectOutputStream(
					baos);
			oos.writeObject(serializableSampleModel);
			sampleModelBinary = baos.toByteArray();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to serialize sample model",
					e);
		}
		final SerializableState serializableColorModel = SerializerFactory.getState(colorModel);
		byte[] colorModelBinary = new byte[0];
		try {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final ObjectOutputStream oos = new ObjectOutputStream(
					baos);
			oos.writeObject(serializableColorModel);
			colorModelBinary = baos.toByteArray();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to serialize sample model",
					e);
		}
		int metadataBinaryLength = 4;
		final List<byte[]> entryBinaries = new ArrayList<byte[]>();
		for (final Entry<String, String> e : metadata.entrySet()) {
			final byte[] keyBytes = StringUtils.stringToBinary(e.getKey());
			final byte[] valueBytes = StringUtils.stringToBinary(e.getValue());

			final int entryBinaryLength = 4 + valueBytes.length + keyBytes.length;
			final ByteBuffer buf = ByteBuffer.allocate(entryBinaryLength);
			buf.putInt(keyBytes.length);
			buf.put(keyBytes);
			buf.put(valueBytes);
			entryBinaries.add(buf.array());
			metadataBinaryLength += (entryBinaryLength + 4);
		}
		byte[] histogramConfigBinary;
		if (histogramConfig != null) {
			histogramConfigBinary = PersistenceUtils.toBinary(histogramConfig);
		}
		else {
			histogramConfigBinary = new byte[] {};
		}
		final byte[] noDataBinary;
		if (noDataValuesPerBand != null) {
			int totalBytes = 4;
			final List<byte[]> noDataValuesBytes = new ArrayList<byte[]>(
					noDataValuesPerBand.length);
			for (final double[] noDataValues : noDataValuesPerBand) {
				final int thisBytes = 4 + (noDataValues.length * 8);
				totalBytes += thisBytes;
				final ByteBuffer noDataBuf = ByteBuffer.allocate(thisBytes);
				noDataBuf.putInt(noDataValues.length);
				for (final double noDataValue : noDataValues) {
					noDataBuf.putDouble(noDataValue);
				}
				noDataValuesBytes.add(noDataBuf.array());
			}
			final ByteBuffer noDataBuf = ByteBuffer.allocate(totalBytes);
			noDataBuf.putInt(noDataValuesPerBand.length);
			for (final byte[] noDataValueBytes : noDataValuesBytes) {
				noDataBuf.put(noDataValueBytes);
			}
			noDataBinary = noDataBuf.array();
		}
		else {
			noDataBinary = new byte[] {};
		}

		final byte[] backgroundBinary;
		if (backgroundValuesPerBand != null) {
			final int totalBytes = (backgroundValuesPerBand.length * 8);
			final ByteBuffer backgroundBuf = ByteBuffer.allocate(totalBytes);
			for (final double backgroundValue : backgroundValuesPerBand) {
				backgroundBuf.putDouble(backgroundValue);
			}
			backgroundBinary = backgroundBuf.array();
		}
		else {
			backgroundBinary = new byte[] {};
		}
		byte[] mergeStrategyBinary;
		if (mergeStrategy != null) {
			mergeStrategyBinary = PersistenceUtils.toBinary(mergeStrategy);
		}
		else {
			mergeStrategyBinary = new byte[] {};
		}
		final ByteBuffer buf = ByteBuffer.allocate(coverageNameBytes.length + sampleModelBinary.length + colorModelBinary.length + metadataBinaryLength + histogramConfigBinary.length + noDataBinary.length + backgroundBinary.length + 29);
		buf.putInt(tileSize);
		buf.putInt(coverageNameBytes.length);
		buf.put(coverageNameBytes);
		buf.putInt(sampleModelBinary.length);
		buf.put(sampleModelBinary);
		buf.putInt(colorModelBinary.length);
		buf.put(colorModelBinary);
		buf.putInt(entryBinaries.size());
		for (final byte[] entryBinary : entryBinaries) {
			buf.putInt(entryBinary.length);
			buf.put(entryBinary);
		}
		buf.putInt(histogramConfigBinary.length);
		buf.put(histogramConfigBinary);
		buf.putInt(noDataBinary.length);
		buf.put(noDataBinary);
		buf.putInt(backgroundBinary.length);
		buf.put(backgroundBinary);
		buf.putInt(mergeStrategyBinary.length);
		buf.put(mergeStrategyBinary);
		buf.put(buildPyramid ? (byte) 1 : (byte) 0);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		tileSize = buf.getInt();
		final int coverageNameLength = buf.getInt();
		final byte[] coverageNameBinary = new byte[coverageNameLength];
		buf.get(coverageNameBinary);
		coverageName = StringUtils.stringFromBinary(coverageNameBinary);

		final int sampleModelLength = buf.getInt();
		final byte[] sampleModelBinary = new byte[sampleModelLength];
		buf.get(sampleModelBinary);
		try {
			final ByteArrayInputStream bais = new ByteArrayInputStream(
					sampleModelBinary);
			final ObjectInputStream ois = new ObjectInputStream(
					bais);
			final Object o = ois.readObject();
			if ((o instanceof SerializableState) && (((SerializableState) o).getObject() instanceof SampleModel)) {
				sampleModel = (SampleModel) ((SerializableState) o).getObject();
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to deserialize sample model",
					e);
		}

		final int colorModelLength = buf.getInt();
		final byte[] colorModelBinary = new byte[colorModelLength];
		buf.get(colorModelBinary);
		try {
			final ByteArrayInputStream bais = new ByteArrayInputStream(
					colorModelBinary);
			final ObjectInputStream ois = new ObjectInputStream(
					bais);
			final Object o = ois.readObject();
			if ((o instanceof SerializableState) && (((SerializableState) o).getObject() instanceof ColorModel)) {
				colorModel = (ColorModel) ((SerializableState) o).getObject();
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to deserialize color model",
					e);
		}
		final int numMetadataEntries = buf.getInt();
		metadata = new HashMap<String, String>();
		for (int i = 0; i < numMetadataEntries; i++) {
			final int entryBinaryLength = buf.getInt();
			final byte[] entryBinary = new byte[entryBinaryLength];
			buf.get(entryBinary);
			final ByteBuffer entryBuf = ByteBuffer.wrap(entryBinary);
			final int keyLength = entryBuf.getInt();
			final byte[] keyBinary = new byte[keyLength];
			final byte[] valueBinary = new byte[entryBinary.length - keyLength];
			entryBuf.get(keyBinary);
			entryBuf.get(valueBinary);
			metadata.put(
					StringUtils.stringFromBinary(keyBinary),
					StringUtils.stringFromBinary(valueBinary));
		}
		final byte[] histogramConfigBinary = new byte[buf.getInt()];
		buf.get(histogramConfigBinary);
		histogramConfig = PersistenceUtils.fromBinary(
				histogramConfigBinary,
				HistogramConfig.class);
		final int noDataBinaryLength = buf.getInt();
		if (noDataBinaryLength == 0) {
			noDataValuesPerBand = null;
		}
		else {
			noDataValuesPerBand = new double[buf.getInt()][];
			for (int b = 0; b < noDataValuesPerBand.length; b++) {
				noDataValuesPerBand[b] = new double[buf.getInt()];
				for (int i = 0; i < noDataValuesPerBand[b].length; i++) {
					noDataValuesPerBand[b][i] = buf.getDouble();
				}
			}
		}
		final int backgroundBinaryLength = buf.getInt();
		if (backgroundBinaryLength == 0) {
			backgroundValuesPerBand = null;
		}
		else {
			backgroundValuesPerBand = new double[backgroundBinaryLength / 8];
			for (int b = 0; b < backgroundValuesPerBand.length; b++) {
				backgroundValuesPerBand[b] = buf.getDouble();
			}
		}
		final byte[] mergeStrategyBinary = new byte[buf.getInt()];
		if (mergeStrategyBinary.length == 0) {
			mergeStrategy = null;
		}
		else {
			buf.get(mergeStrategyBinary);
			mergeStrategy = PersistenceUtils.fromBinary(
					mergeStrategyBinary,
					RasterTileMergeStrategy.class);
		}
		buildPyramid = (buf.get() != 0);
		init();
	}

	@Override
	public FieldWriter<GridCoverage, Object> getWriter(
			final ByteArrayId fieldId ) {
		if (DATA_FIELD_ID.equals(fieldId)) {
			return (FieldWriter) new RasterTileWriter();
		}
		return null;
	}

	@Override
	public ByteArrayId[] getSupportedStatisticsIds() {
		return supportedStatsIds;
	}

	@Override
	public DataStatistics<GridCoverage> createDataStatistics(
			final ByteArrayId statisticsId ) {
		if (OverviewStatistics.STATS_ID.equals(statisticsId)) {
			return new OverviewStatistics(
					new ByteArrayId(
							coverageName));
		}
		else if (BoundingBoxDataStatistics.STATS_ID.equals(statisticsId)) {
			return new RasterBoundingBoxStatistics(
					new ByteArrayId(
							coverageName));
		}
		else if (RasterFootprintStatistics.STATS_ID.equals(statisticsId)) {
			return new RasterBoundingBoxStatistics(
					new ByteArrayId(
							coverageName));
		}
		else if (HistogramStatistics.STATS_ID.equals(statisticsId) && (histogramConfig != null)) {
			return new HistogramStatistics(
					new ByteArrayId(
							coverageName),
					histogramConfig);
		}
		return null;
	}

	public double[][] getNoDataValuesPerBand() {
		return noDataValuesPerBand;
	}

	@Override
	public DataStatisticsVisibilityHandler<GridCoverage> getVisibilityHandler(
			final ByteArrayId statisticsId ) {
		return visibilityHandler;
	}

	public Map<String, String> getMetadata() {
		return metadata;
	}

	public String getCoverageName() {
		return coverageName;
	}

	public SampleModel getSampleModel() {
		return sampleModel;
	}

	public ColorModel getColorModel() {
		return colorModel;
	}

	public int getTileSize() {
		return tileSize;
	}

	private static final class SimplifiedGridSampleDimension extends
			GridSampleDimension implements
			SampleDimension
	{

		/**
			 *
			 */
		private static final long serialVersionUID = 2227219522016820587L;

		private final double nodata;
		private final double minimum;
		private final double maximum;
		private final double scale;
		private final double offset;
		private final Unit<?> unit;
		private final SampleDimensionType type;
		private final ColorInterpretation color;
		private final Category bkg;

		public SimplifiedGridSampleDimension(
				final CharSequence description,
				final SampleDimensionType type,
				final ColorInterpretation color,
				final double nodata,
				final double minimum,
				final double maximum,
				final double scale,
				final double offset,
				final Unit<?> unit ) {
			super(
					description,
					!Double.isNaN(nodata) ? new Category[] {
						new Category(
								Vocabulary.formatInternational(VocabularyKeys.NODATA),
								new Color[] {
									new Color(
											0,
											0,
											0,
											0)
								},
								NumberRange.create(
										nodata,
										nodata),
								NumberRange.create(
										nodata,
										nodata))
					} : null,
					unit);
			this.nodata = nodata;
			this.minimum = minimum;
			this.maximum = maximum;
			this.scale = scale;
			this.offset = offset;
			this.unit = unit;
			this.type = type;
			this.color = color;
			bkg = new Category(
					"Background",
					TRANSPARENT,
					0);
		}

		@Override
		public double getMaximumValue() {
			return maximum;
		}

		@Override
		public double getMinimumValue() {
			return minimum;
		}

		@Override
		public double[] getNoDataValues()
				throws IllegalStateException {
			return new double[] {
				nodata
			};
		}

		@Override
		public double getOffset()
				throws IllegalStateException {
			return offset;
		}

		@Override
		public NumberRange<? extends Number> getRange() {
			return super.getRange();
		}

		@Override
		public SampleDimensionType getSampleDimensionType() {
			return type;
		}

		@Override
		public MathTransform1D getSampleToGeophysics() {
			return super.getSampleToGeophysics();
		}

		@Override
		public Unit<?> getUnits() {
			return unit;
		}

		@Override
		public double getScale() {
			return scale;
		}

		@Override
		public ColorInterpretation getColorInterpretation() {
			return color;
		}

		@Override
		public Category getBackground() {
			return bkg;
		}

		@Override
		public InternationalString[] getCategoryNames()
				throws IllegalStateException {
			return new InternationalString[] {
				SimpleInternationalString.wrap("Background")
			};
		}
	}

	private static class InternalWritableRaster extends
			WritableRaster
	{
		// the constructor is protected, so this class is intended as a simple
		// way to access the constructor
		protected InternalWritableRaster(
				final SampleModel sampleModel,
				final Point origin ) {
			super(
					sampleModel,
					origin);
		}

	}

	@Override
	public IteratorConfig[] getAttachedIteratorConfig() {
		final RasterTileCombinerConfig tileCombiner = new RasterTileCombinerConfig(
				new IteratorSetting(
						RASTER_TILE_COMBINER_PRIORITY,
						RasterTileCombiner.class),
				EnumSet.allOf(IteratorScope.class));
		final List<Column> columns = new ArrayList<Column>();
		columns.add(new Column(
				getCoverageName()));
		Combiner.setColumns(
				tileCombiner.getIteratorSettings(),
				columns);
		final String mergeStrategyStr = ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(mergeStrategy));
		tileCombiner.getIteratorSettings().addOption(
				RasterTileCombinerHelper.MERGE_STRATEGY_KEY,
				mergeStrategyStr);
		final RasterTileCombinerConfig tileVisibilityCombiner = new RasterTileCombinerConfig(
				new IteratorSetting(
						RASTER_TILE_VISIBILITY_COMBINER_PRIORITY,
						RasterTileVisibilityCombiner.class),
				EnumSet.allOf(IteratorScope.class));
		tileVisibilityCombiner.getIteratorSettings().addOption(
				RasterTileCombinerHelper.MERGE_STRATEGY_KEY,
				mergeStrategyStr);
		Combiner.setColumns(
				tileVisibilityCombiner.getIteratorSettings(),
				columns);
		return new IteratorConfig[] {
			tileCombiner,
			tileVisibilityCombiner
		};
	}

}
