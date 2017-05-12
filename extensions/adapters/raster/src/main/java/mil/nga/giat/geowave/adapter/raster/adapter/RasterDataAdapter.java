package mil.nga.giat.geowave.adapter.raster.adapter;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import javax.measure.unit.Unit;
import javax.media.jai.Interpolation;
import javax.media.jai.InterpolationBicubic2;
import javax.media.jai.InterpolationBilinear;
import javax.media.jai.InterpolationNearest;
import javax.media.jai.PlanarImage;
import javax.media.jai.remote.SerializableState;
import javax.media.jai.remote.SerializerFactory;

import org.apache.commons.math.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.geotools.referencing.operation.projection.MapProjection;
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
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.InternationalString;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

import mil.nga.giat.geowave.adapter.raster.FitToIndexGridCoverage;
import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.adapter.raster.Resolution;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileRowTransform;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.RootMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.adapter.warp.WarpRIF;
import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveGTRasterFormat;
import mil.nga.giat.geowave.adapter.raster.stats.HistogramConfig;
import mil.nga.giat.geowave.adapter.raster.stats.HistogramStatistics;
import mil.nga.giat.geowave.adapter.raster.stats.OverviewStatistics;
import mil.nga.giat.geowave.adapter.raster.stats.RasterBoundingBoxStatistics;
import mil.nga.giat.geowave.adapter.raster.stats.RasterFootprintStatistics;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.CompoundIndexStrategy;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.FitToIndexPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.FieldIdStatisticVisibility;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.IteratorWrapper;
import mil.nga.giat.geowave.core.store.util.IteratorWrapper.Converter;
import mil.nga.giat.geowave.mapreduce.HadoopDataAdapter;
import mil.nga.giat.geowave.mapreduce.HadoopWritableSerializer;

public class RasterDataAdapter implements
		StatisticsProvider<GridCoverage>,
		IndexDependentDataAdapter<GridCoverage>,
		HadoopDataAdapter<GridCoverage, GridCoverageWritable>,
		RowMergingDataAdapter<GridCoverage, RasterTile<?>>
{
	static {
		SourceThresholdFixMosaicDescriptor.register(false);
		WarpRIF.register(false);
		MapProjection.SKIP_SANITY_CHECKS = true;
	}

	public final static String TILE_METADATA_PROPERTY_KEY = "TILE_METADATA";

	private final static Logger LOGGER = LoggerFactory.getLogger(RasterDataAdapter.class);
	private final static ByteArrayId DATA_FIELD_ID = new ByteArrayId(
			"image");
	public final static int DEFAULT_TILE_SIZE = 256;
	public final static boolean DEFAULT_BUILD_PYRAMID = false;
	public final static boolean DEFAULT_BUILD_HISTOGRAM = true;

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
	private double[] minsPerBand;
	private double[] maxesPerBand;
	private String[] namesPerBand;
	private double[] backgroundValuesPerBand;
	private boolean buildPyramid;
	private ByteArrayId[] supportedStatsTypes;
	private EntryVisibilityHandler<GridCoverage> visibilityHandler;
	private RootMergeStrategy<?> mergeStrategy;
	private boolean equalizeHistogram;
	private Interpolation interpolation;

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
				DEFAULT_BUILD_HISTOGRAM,
				new double[originalGridCoverage.getNumSampleDimensions()][],
				new NoDataMergeStrategy());
	}

	public RasterDataAdapter(
			final String coverageName,
			final Map<String, String> metadata,
			final GridCoverage2D originalGridCoverage,
			final int tileSize,
			final boolean buildPyramid ) {
		this(
				coverageName,
				metadata,
				originalGridCoverage,
				tileSize,
				buildPyramid,
				DEFAULT_BUILD_HISTOGRAM,
				new double[originalGridCoverage.getNumSampleDimensions()][],
				new NoDataMergeStrategy());
	}

	public RasterDataAdapter(
			final String coverageName,
			final Map<String, String> metadata,
			final GridCoverage2D originalGridCoverage,
			final int tileSize,
			final boolean buildPyramid,
			final boolean buildHistogram,
			final double[][] noDataValuesPerBand ) {
		this(
				coverageName,
				metadata,
				originalGridCoverage,
				tileSize,
				buildPyramid,
				buildHistogram,
				noDataValuesPerBand,
				new NoDataMergeStrategy());
	}

	public RasterDataAdapter(
			final String coverageName,
			final Map<String, String> metadata,
			final GridCoverage2D originalGridCoverage,
			final int tileSize,
			final boolean buildPyramid,
			final boolean buildHistogram,
			final double[][] noDataValuesPerBand,
			final RasterTileMergeStrategy<?> mergeStrategy ) {
		final RenderedImage img = originalGridCoverage.getRenderedImage();
		sampleModel = img.getSampleModel();
		colorModel = img.getColorModel();
		this.metadata = metadata;
		this.coverageName = coverageName;
		this.tileSize = tileSize;
		if (buildHistogram) {
			histogramConfig = new HistogramConfig(
					sampleModel);
		}
		else {
			histogramConfig = null;
		}
		if ((noDataValuesPerBand != null) && (noDataValuesPerBand.length != 0)) {
			this.noDataValuesPerBand = noDataValuesPerBand;
			backgroundValuesPerBand = new double[noDataValuesPerBand.length];
			for (int d = 0; d < this.noDataValuesPerBand.length; d++) {
				if ((noDataValuesPerBand[d] != null) && (noDataValuesPerBand[d].length > 0)) {
					backgroundValuesPerBand[d] = noDataValuesPerBand[d][0];
				}
				else {
					backgroundValuesPerBand[d] = 0.0;
				}
			}
		}
		else {
			this.noDataValuesPerBand = new double[originalGridCoverage.getNumSampleDimensions()][];
			for (int d = 0; d < this.noDataValuesPerBand.length; d++) {
				this.noDataValuesPerBand[d] = originalGridCoverage.getSampleDimension(
						d).getNoDataValues();
			}
			backgroundValuesPerBand = CoverageUtilities.getBackgroundValues(originalGridCoverage);
		}

		this.buildPyramid = buildPyramid;
		if (mergeStrategy != null) {
			this.mergeStrategy = new RootMergeStrategy(
					getAdapterId(),
					sampleModel.createCompatibleSampleModel(
							tileSize,
							tileSize),
					mergeStrategy);
		}
		else {
			this.mergeStrategy = null;
		}
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
				true,
				Interpolation.INTERP_NEAREST,
				buildPyramid,
				new NoDataMergeStrategy());
	}

	public RasterDataAdapter(
			final RasterDataAdapter adapter,
			final String coverageName ) {
		this(
				adapter,
				coverageName,
				adapter.tileSize);
	}

	public RasterDataAdapter(
			final RasterDataAdapter adapter,
			final String coverageName,
			final int tileSize ) {
		this(
				coverageName,
				adapter.getSampleModel().createCompatibleSampleModel(
						tileSize,
						tileSize),
				adapter.getColorModel(),
				adapter.getMetadata(),
				tileSize,
				adapter.getNoDataValuesPerBand(),
				adapter.backgroundValuesPerBand,
				adapter.histogramConfig,
				adapter.equalizeHistogram,
				interpolationToByte(adapter.interpolation),
				adapter.buildPyramid,
				adapter.mergeStrategy == null ? null : adapter.mergeStrategy.getChildMergeStrategy(adapter
						.getAdapterId()));
	}

	public RasterDataAdapter(
			final RasterDataAdapter adapter,
			final String coverageName,
			final RasterTileMergeStrategy<?> mergeStrategy ) {
		this(
				coverageName,
				adapter.getSampleModel(),
				adapter.getColorModel(),
				adapter.getMetadata(),
				adapter.tileSize,
				null,
				null,
				null,
				adapter.getNoDataValuesPerBand(),
				adapter.backgroundValuesPerBand,
				adapter.histogramConfig,
				adapter.equalizeHistogram,
				interpolationToByte(adapter.interpolation),
				adapter.buildPyramid,
				mergeStrategy);
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
			final boolean equalizeHistogram,
			final int interpolationType,
			final boolean buildPyramid,
			final RasterTileMergeStrategy<?> mergeStrategy ) {
		this(
				coverageName,
				sampleModel,
				colorModel,
				metadata,
				tileSize,
				null,
				null,
				null,
				noDataValuesPerBand,
				backgroundValuesPerBand,
				histogramConfig,
				equalizeHistogram,
				interpolationType,
				buildPyramid,
				mergeStrategy);
	}

	public RasterDataAdapter(
			final String coverageName,
			final SampleModel sampleModel,
			final ColorModel colorModel,
			final Map<String, String> metadata,
			final int tileSize,
			final double[] minsPerBand,
			final double[] maxesPerBand,
			final String[] namesPerBand,
			final double[][] noDataValuesPerBand,
			final double[] backgroundValuesPerBand,
			final HistogramConfig histogramConfig,
			final boolean equalizeHistogram,
			final int interpolationType,
			final boolean buildPyramid,
			final RasterTileMergeStrategy<?> mergeStrategy ) {
		this.coverageName = coverageName;
		this.tileSize = tileSize;
		this.sampleModel = sampleModel;
		this.colorModel = colorModel;
		this.metadata = metadata;
		this.minsPerBand = minsPerBand;
		this.maxesPerBand = maxesPerBand;
		this.namesPerBand = namesPerBand;
		this.noDataValuesPerBand = noDataValuesPerBand;
		this.backgroundValuesPerBand = backgroundValuesPerBand;
		// a null histogram config will result in histogram statistics not being
		// accumulated
		this.histogramConfig = histogramConfig;
		this.buildPyramid = buildPyramid;
		this.equalizeHistogram = equalizeHistogram;
		interpolation = Interpolation.getInstance(interpolationType);
		if (mergeStrategy != null) {
			this.mergeStrategy = new RootMergeStrategy(
					getAdapterId(),
					sampleModel.createCompatibleSampleModel(
							tileSize,
							tileSize),
					mergeStrategy);
		}
		else {
			this.mergeStrategy = null;
		}
		init();
	}

	private void init() {
		int supportedStatsLength = 2;

		if (histogramConfig != null) {
			supportedStatsLength++;
		}

		supportedStatsTypes = new ByteArrayId[supportedStatsLength];
		supportedStatsTypes[0] = OverviewStatistics.STATS_TYPE;
		supportedStatsTypes[1] = BoundingBoxDataStatistics.STATS_TYPE;

		if (histogramConfig != null) {
			supportedStatsTypes[2] = HistogramStatistics.STATS_TYPE;
		}
		visibilityHandler = new FieldIdStatisticVisibility<GridCoverage>(
				DATA_FIELD_ID);
	}

	@Override
	public Iterator<GridCoverage> convertToIndex(
			final PrimaryIndex index,
			final GridCoverage gridCoverage ) {
		final HierarchicalNumericIndexStrategy indexStrategy = CompoundHierarchicalIndexStrategyWrapper
				.findHierarchicalStrategy(index.getIndexStrategy());
		if (indexStrategy != null) {
			final CoordinateReferenceSystem sourceCrs = gridCoverage.getCoordinateReferenceSystem();

			final Envelope sampleEnvelope = gridCoverage.getEnvelope();

			final ReferencedEnvelope sampleReferencedEnvelope = new ReferencedEnvelope(
					new com.vividsolutions.jts.geom.Envelope(
							sampleEnvelope.getMinimum(0),
							sampleEnvelope.getMaximum(0),
							sampleEnvelope.getMinimum(1),
							sampleEnvelope.getMaximum(1)),
					gridCoverage.getCoordinateReferenceSystem());

			ReferencedEnvelope projectedReferenceEnvelope = sampleReferencedEnvelope;
			if (!GeoWaveGTRasterFormat.DEFAULT_CRS.equals(sourceCrs)) {
				try {
					projectedReferenceEnvelope = sampleReferencedEnvelope.transform(
							GeoWaveGTRasterFormat.DEFAULT_CRS,
							true);
				}
				catch (TransformException | FactoryException e) {
					LOGGER.warn(
							"Unable to transform envelope of grid coverage to EPSG:4326",
							e);
				}
			}

			final MultiDimensionalNumericData bounds = GeometryUtils.basicConstraintSetFromEnvelope(
					projectedReferenceEnvelope).getIndexConstraints(
					indexStrategy);
			final GridEnvelope gridEnvelope = gridCoverage.getGridGeometry().getGridRange();
			// only one set of constraints..hence reference '0' element
			final double[] tileRangePerDimension = new double[bounds.getDimensionCount()];
			final double[] maxValuesPerDimension = bounds.getMaxValuesPerDimension();
			final double[] minValuesPerDimension = bounds.getMinValuesPerDimension();
			for (int d = 0; d < tileRangePerDimension.length; d++) {
				tileRangePerDimension[d] = ((maxValuesPerDimension[d] - minValuesPerDimension[d]) * tileSize)
						/ gridEnvelope.getSpan(d);
			}
			final TreeMap<Double, SubStrategy> substrategyMap = new TreeMap<Double, SubStrategy>();
			for (final SubStrategy pyramidLevel : indexStrategy.getSubStrategies()) {
				final double[] idRangePerDimension = pyramidLevel
						.getIndexStrategy()
						.getHighestPrecisionIdRangePerDimension();
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
			// all entries will be greater than 1 (lower resolution pyramid
			// levels)
			// also try to find the one entry that is closest to 1.0 without
			// going over (this will be the full resolution level)
			// add an epsilon to try to catch any roundoff error
			final double fullRes = 1.0 + MathUtils.EPSILON;
			final Entry<Double, SubStrategy> fullResEntry = substrategyMap.floorEntry(fullRes);
			final List<SubStrategy> pyramidLevels = new ArrayList<SubStrategy>();
			if (fullResEntry != null) {
				pyramidLevels.add(fullResEntry.getValue());
			}
			if (buildPyramid) {
				NavigableMap<Double, SubStrategy> map = substrategyMap.tailMap(
						fullRes,
						false);
				pyramidLevels.addAll(map.values());
			}
			if (pyramidLevels.isEmpty()) {
				// this case shouldn't occur theoretically, but just in case,
				// make sure the substrategy closest to 1.0 is used
				final Entry<Double, SubStrategy> bestEntry = substrategyMap.higherEntry(1.0);
				pyramidLevels.add(bestEntry.getValue());
			}
			final SubStrategy pyramidLevel = pyramidLevels.get(0);
			final double[] idRangePerDimension = pyramidLevel
					.getIndexStrategy()
					.getHighestPrecisionIdRangePerDimension();
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
			return new IteratorWrapper<SubStrategy, GridCoverage>(
					pyramidLevels.iterator(),
					new MosaicPerPyramidLevelBuilder(
							bounds,
							gridCoverage,
							tileSize,
							backgroundValuesPerBand,
							RasterUtils.getFootprint(
									projectedReferenceEnvelope,
									gridCoverage),
							interpolation));
		}
		LOGGER.warn("Strategy is not an instance of HierarchicalNumericIndexStrategy : "
				+ index.getIndexStrategy().getClass().getName());
		return Collections.<GridCoverage> emptyList().iterator();
	}

	private static class MosaicPerPyramidLevelBuilder implements
			Converter<SubStrategy, GridCoverage>
	{
		private final MultiDimensionalNumericData originalBounds;
		private final GridCoverage originalData;
		private final int tileSize;
		private final double[] backgroundValuesPerBand;
		private final Geometry footprint;
		private final Interpolation defaultInterpolation;

		public MosaicPerPyramidLevelBuilder(
				final MultiDimensionalNumericData originalBounds,
				final GridCoverage originalData,
				final int tileSize,
				final double[] backgroundValuesPerBand,
				final Geometry footprint,
				final Interpolation defaultInterpolation ) {
			this.originalBounds = originalBounds;
			this.originalData = originalData;
			this.tileSize = tileSize;
			this.backgroundValuesPerBand = backgroundValuesPerBand;
			this.footprint = footprint;
			this.defaultInterpolation = defaultInterpolation;
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
					ByteArrayId insertionId = insertionIds.next();
					if (insertionId == null) {
						return null;
					}
					final MultiDimensionalNumericData rangePerDimension = pyramidLevel
							.getIndexStrategy()
							.getRangeForId(
									insertionId);
					final NumericDimensionDefinition[] dimensions = pyramidLevel
							.getIndexStrategy()
							.getOrderedDimensionDefinitions();
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

						final double[] tileRes = pyramidLevel
								.getIndexStrategy()
								.getHighestPrecisionIdRangePerDimension();
						final double[] pixelRes = new double[tileRes.length];
						for (int d = 0; d < tileRes.length; d++) {
							pixelRes[d] = tileRes[d] / tileSize;
						}
						Geometry footprintWithinTileWorldGeom = null;
						Geometry footprintWithinTileScreenGeom = null;
						try {
							// using fixed precision for geometry factory will
							// round screen geometry values to the nearest
							// pixel, which seems to be the most appropriate
							// behavior
							final Geometry wholeFootprintScreenGeom = new GeometryFactory(
									new PrecisionModel(
											PrecisionModel.FIXED)).createGeometry(JTS.transform(
									footprint,
									new AffineTransform2D(
											worldToScreenTransform)));
							final com.vividsolutions.jts.geom.Envelope fullTileEnvelope = new com.vividsolutions.jts.geom.Envelope(
									0,
									tileSize,
									0,
									tileSize);
							final GeometryClipper tileClipper = new GeometryClipper(
									fullTileEnvelope);
							footprintWithinTileScreenGeom = tileClipper.clip(
									wholeFootprintScreenGeom,
									true);
							if (footprintWithinTileScreenGeom == null) {
								// for some reason the original image
								// footprint
								// falls outside this insertion ID
								LOGGER.warn("Original footprint geometry (" + originalData.getGridGeometry()
										+ ") falls outside the insertion bounds (" + insertionIdGeometry + ")");
								return null;
							}
							footprintWithinTileWorldGeom = JTS.transform(
									// change the precision model back to JTS
									// default from fixed precision
									new GeometryFactory().createGeometry(footprintWithinTileScreenGeom),
									gridToCRS);

							if (footprintWithinTileScreenGeom
									.covers(new GeometryFactory().toGeometry(fullTileEnvelope))) {
								// if the screen geometry fully covers the
								// tile,
								// don't bother carrying it forward
								footprintWithinTileScreenGeom = null;
							}
						}
						catch (final TransformException e) {
							LOGGER.warn(
									"Unable to calculate geometry of footprint for tile",
									e);
						}

						Interpolation tileInterpolation = defaultInterpolation;
						final int dataType = originalData.getRenderedImage().getSampleModel().getDataType();

						// TODO a JAI bug "workaround" in GeoTools does not
						// work, this is a workaround for the GeoTools bug
						// see https://jira.codehaus.org/browse/GEOT-3585,
						// and
						// line 666-698 of
						// org.geotools.coverage.processing.operation.Resampler2D
						// (gt-coverage-12.1)
						if ((dataType == DataBuffer.TYPE_FLOAT) || (dataType == DataBuffer.TYPE_DOUBLE)) {
							final Envelope tileEnvelope = insertionIdGeometry.getEnvelope();
							final ReferencedEnvelope tileReferencedEnvelope = new ReferencedEnvelope(
									new com.vividsolutions.jts.geom.Envelope(
											tileEnvelope.getMinimum(0),
											tileEnvelope.getMaximum(0),
											tileEnvelope.getMinimum(1),
											tileEnvelope.getMaximum(1)),
									GeoWaveGTRasterFormat.DEFAULT_CRS);
							final Geometry tileJTSGeometry = new GeometryFactory().toGeometry(tileReferencedEnvelope);
							if (!footprint.contains(tileJTSGeometry)) {
								tileInterpolation = Interpolation.getInstance(Interpolation.INTERP_NEAREST);
							}
						}
						GridCoverage resampledCoverage = (GridCoverage) RasterUtils.getCoverageOperations().resample(
								originalData,
								GeoWaveGTRasterFormat.DEFAULT_CRS,
								insertionIdGeometry,
								tileInterpolation,
								backgroundValuesPerBand);
						// NOTE: for now this is commented out, but
						// beware the
						// resample operation under certain conditions,
						// this requires more investigation rather than
						// adding a
						// hacky fix

						// sometimes the resample results in an image that
						// is
						// not tileSize in width and height although the
						// insertionIdGeometry is telling it to resample to
						// tileSize

						// in these cases, check and perform a rescale to
						// finalize the grid coverage to guarantee it is the
						// correct tileSize

						final GridEnvelope e = resampledCoverage.getGridGeometry().getGridRange();
						boolean resize = false;

						for (int d = 0; d < e.getDimension(); d++) {
							if (e.getSpan(d) != tileSize) {
								resize = true;
								break;
							}
						}
						if (resize) {
							resampledCoverage = Operations.DEFAULT.scale(
									resampledCoverage,
									(double) tileSize / (double) e.getSpan(0),
									(double) tileSize / (double) e.getSpan(1),
									-resampledCoverage.getRenderedImage().getMinX(),
									-resampledCoverage.getRenderedImage().getMinY());
						}
						if ((resampledCoverage.getRenderedImage().getWidth() != tileSize)
								|| (resampledCoverage.getRenderedImage().getHeight() != tileSize)
								|| (resampledCoverage.getRenderedImage().getMinX() != 0)
								|| (resampledCoverage.getRenderedImage().getMinY() != 0)) {
							resampledCoverage = Operations.DEFAULT.scale(
									resampledCoverage,
									1,
									1,
									-resampledCoverage.getRenderedImage().getMinX(),
									-resampledCoverage.getRenderedImage().getMinY());
						}
						if (pyramidLevel.getIndexStrategy() instanceof CompoundIndexStrategy) {
							// this is exclusive on the end, and the tier is set
							// so just get the id based on the lowest half of
							// the multidimensional data
							final double[] centroids = rangePerDimension.getCentroidPerDimension();
							final double[] mins = rangePerDimension.getMinValuesPerDimension();
							final NumericRange[] ranges = new NumericRange[centroids.length];
							for (int d = 0; d < centroids.length; d++) {
								ranges[d] = new NumericRange(
										mins[d],
										centroids[d]);
							}

							insertionId = pyramidLevel.getIndexStrategy().getInsertionIds(
									new BasicNumericDataset(
											ranges)).get(
									0);
							// this is intended to allow the partitioning
							// algorithm to use a consistent multi-dimensional
							// dataset (so if hashing is done on the
							// multi-dimensional data, it will be a consistent
							// hash for each tile and merge strategies will work
							// correctly)
						}
						return new FitToIndexGridCoverage(
								resampledCoverage,
								insertionId,
								new Resolution(
										pixelRes),
								originalEnvelope,
								footprintWithinTileWorldGeom,
								footprintWithinTileScreenGeom,
								getProperties(originalData));
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
			final PrimaryIndex index ) {
		final Object rasterTile = data.getAdapterExtendedData().getValue(
				DATA_FIELD_ID);
		if ((rasterTile == null) || !(rasterTile instanceof RasterTile)) {
			return null;
		}
		return getCoverageFromRasterTile(
				(RasterTile) rasterTile,
				data.getIndexInsertionId(),
				index);
	}

	public GridCoverage getCoverageFromRasterTile(
			final RasterTile rasterTile,
			final ByteArrayId insertionId,
			final PrimaryIndex index ) {
		final MultiDimensionalNumericData indexRange = index.getIndexStrategy().getRangeForId(
				insertionId);
		final NumericDimensionDefinition[] orderedDimensions = index
				.getIndexStrategy()
				.getOrderedDimensionDefinitions();

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
					rasterTile,
					tileSize,
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
			final RasterTile rasterTile,
			final int tileSize,
			final ReferencedEnvelope mapExtent )
			throws IOException {
		final DataBuffer dataBuffer = rasterTile.getDataBuffer();
		final Persistable tileMetadata = rasterTile.getMetadata();
		final SampleModel sm = sampleModel.createCompatibleSampleModel(
				tileSize,
				tileSize);

		final boolean alphaPremultiplied = colorModel.isAlphaPremultiplied();

		final WritableRaster raster = Raster.createWritableRaster(
				sm,
				dataBuffer,
				null);
		final int numBands = sm.getNumBands();
		final BufferedImage image = new BufferedImage(
				colorModel,
				raster,
				alphaPremultiplied,
				null);
		// creating bands
		final ColorModel cm = image.getColorModel();
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

			if (st == null) {
				LOGGER.error("Could not get sample dimension type, getSampleDimensionType returned null");
				throw new IOException(
						"Could not get sample dimension type, getSampleDimensionType returned null");
			}

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

			if ((noDataValuesPerBand != null) && (noDataValuesPerBand[i] != null)
					&& (noDataValuesPerBand[i].length > 0)) {
				// just take the first value, even if there are multiple
				noData = noDataValuesPerBand[i][0];
			}
			if ((minsPerBand != null) && (minsPerBand.length > i)) {
				min = minsPerBand[i];
			}
			if ((maxesPerBand != null) && (maxesPerBand.length > i)) {
				max = maxesPerBand[i];
			}
			if ((namesPerBand != null) && (namesPerBand.length > i)) {
				bandName = namesPerBand[i];
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
					null);
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
			final Map properties = new HashMap();
			if (metadata != null) {
				properties.putAll(metadata);
			}
			if (tileMetadata != null) {
				properties.put(
						TILE_METADATA_PROPERTY_KEY,
						tileMetadata);
			}
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
					properties);
		}
		catch (IllegalArgumentException | NoninvertibleTransformException e) {
			LOGGER.warn(
					"Unable to calculate transformation for grid coordinates on read",
					e);
		}
		return null;
	}

	private static Map getProperties(
			final GridCoverage entry ) {
		Map originalCoverageProperties = new HashMap<>();
		if (entry instanceof GridCoverage2D) {
			originalCoverageProperties = ((GridCoverage2D) entry).getProperties();
		}
		else if (entry instanceof FitToIndexGridCoverage) {
			originalCoverageProperties = ((FitToIndexGridCoverage) entry).getProperties();
		}
		return originalCoverageProperties;
	}

	public MergeableRasterTile<?> getRasterTileFromCoverage(
			final GridCoverage entry ) {
		return new MergeableRasterTile(
				getRaster(
						entry).getDataBuffer(),
				mergeStrategy == null ? null : mergeStrategy.getMetadata(
						entry,
						getProperties(entry),
						this),
				mergeStrategy,
				getAdapterId());
	}

	public Raster getRaster(
			final GridCoverage entry ) {
		final SampleModel sm = sampleModel.createCompatibleSampleModel(
				tileSize,
				tileSize);

		return entry.getRenderedImage().copyData(
				new InternalWritableRaster(
						sm,
						new Point()));
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final GridCoverage entry,
			final CommonIndexModel indexModel ) {
		final PersistentDataset<Object> adapterExtendedData = new PersistentDataset<Object>();
		adapterExtendedData.addValue(new PersistentValue<Object>(
				DATA_FIELD_ID,
				getRasterTileFromCoverage(entry)));
		final AdapterPersistenceEncoding encoding;
		if (entry instanceof FitToIndexGridCoverage) {
			encoding = new FitToIndexPersistenceEncoding(
					getAdapterId(),
					new ByteArrayId(
							new byte[] {}),
					new PersistentDataset<CommonIndexValue>(),
					adapterExtendedData,
					((FitToIndexGridCoverage) entry).getInsertionId());
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
		final byte[] sampleModelBinary = getSampleModelBinary(sampleModel);
		final byte[] colorModelBinary = getColorModelBinary(colorModel);
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
		final byte[] noDataBinary = getNoDataBinary(noDataValuesPerBand);

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
		final byte[] minsBinary;
		if (minsPerBand != null) {
			final int totalBytes = (minsPerBand.length * 8);
			final ByteBuffer minsBuf = ByteBuffer.allocate(totalBytes);
			for (final double min : minsPerBand) {
				minsBuf.putDouble(min);
			}
			minsBinary = minsBuf.array();
		}
		else {
			minsBinary = new byte[] {};
		}
		final byte[] maxesBinary;
		if (maxesPerBand != null) {
			final int totalBytes = (maxesPerBand.length * 8);
			final ByteBuffer maxesBuf = ByteBuffer.allocate(totalBytes);
			for (final double max : maxesPerBand) {
				maxesBuf.putDouble(max);
			}
			maxesBinary = maxesBuf.array();
		}
		else {
			maxesBinary = new byte[] {};
		}

		final byte[] namesBinary;
		final int namesLength;
		if (namesPerBand != null) {
			int totalBytes = 0;
			final List<byte[]> namesBinaries = new ArrayList<byte[]>(
					namesPerBand.length);
			for (final String name : namesPerBand) {
				final byte[] nameBinary = StringUtils.stringToBinary(name);
				final int size = nameBinary.length + 4;
				final ByteBuffer nameBuf = ByteBuffer.allocate(size);
				totalBytes += size;
				nameBuf.putInt(nameBinary.length);
				nameBuf.put(nameBinary);
				namesBinaries.add(nameBuf.array());
			}
			final ByteBuffer namesBuf = ByteBuffer.allocate(totalBytes);
			for (final byte[] nameBinary : namesBinaries) {
				namesBuf.put(nameBinary);
			}
			namesBinary = namesBuf.array();
			namesLength = namesPerBand.length;
		}
		else {
			namesBinary = new byte[] {};
			namesLength = 0;
		}
		byte[] mergeStrategyBinary;
		if (mergeStrategy != null) {
			mergeStrategyBinary = PersistenceUtils.toBinary(mergeStrategy);
		}
		else {
			mergeStrategyBinary = new byte[] {};
		}
		final ByteBuffer buf = ByteBuffer.allocate(coverageNameBytes.length + sampleModelBinary.length
				+ colorModelBinary.length + metadataBinaryLength + histogramConfigBinary.length + noDataBinary.length
				+ minsBinary.length + maxesBinary.length + namesBinary.length + backgroundBinary.length
				+ mergeStrategyBinary.length + 47);
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
		buf.putInt(minsBinary.length);
		buf.put(minsBinary);
		buf.putInt(maxesBinary.length);
		buf.put(maxesBinary);
		buf.putInt(namesLength);
		buf.put(namesBinary);
		buf.putInt(backgroundBinary.length);
		buf.put(backgroundBinary);
		buf.putInt(mergeStrategyBinary.length);
		buf.put(mergeStrategyBinary);
		buf.put(buildPyramid ? (byte) 1 : (byte) 0);
		buf.put(equalizeHistogram ? (byte) 1 : (byte) 0);
		buf.put(interpolationToByte(interpolation));
		return buf.array();
	}

	protected static byte interpolationToByte(
			final Interpolation interpolation ) {
		// this is silly because it seems like a translation JAI should provide,
		// but it seems its not provided and its the most efficient approach
		// (rather than serializing class names)
		if (interpolation instanceof InterpolationNearest) {
			return Interpolation.INTERP_NEAREST;
		}
		if (interpolation instanceof InterpolationBilinear) {
			return Interpolation.INTERP_BILINEAR;
		}
		if (interpolation instanceof InterpolationBicubic2) {
			return Interpolation.INTERP_BICUBIC_2;
		}

		return Interpolation.INTERP_BICUBIC;
	}

	protected static byte[] getColorModelBinary(
			final ColorModel colorModel ) {
		final SerializableState serializableColorModel = SerializerFactory.getState(colorModel);
		try {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final ObjectOutputStream oos = new ObjectOutputStream(
					baos);
			oos.writeObject(serializableColorModel);
			return baos.toByteArray();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to serialize sample model",
					e);
		}
		return new byte[] {};
	}

	protected static byte[] getSampleModelBinary(
			final SampleModel sampleModel ) {
		final SerializableState serializableSampleModel = SerializerFactory.getState(sampleModel);
		try {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final ObjectOutputStream oos = new ObjectOutputStream(
					baos);
			oos.writeObject(serializableSampleModel);
			return baos.toByteArray();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to serialize sample model",
					e);
		}
		return new byte[] {};
	}

	protected static byte[] getNoDataBinary(
			final double[][] noDataValuesPerBand ) {
		if (noDataValuesPerBand != null) {
			int totalBytes = 4;
			final List<byte[]> noDataValuesBytes = new ArrayList<byte[]>(
					noDataValuesPerBand.length);
			for (final double[] noDataValues : noDataValuesPerBand) {
				int length = 0;
				if (noDataValues != null) {
					length = noDataValues.length;
				}
				final int thisBytes = 4 + (length * 8);
				totalBytes += thisBytes;
				final ByteBuffer noDataBuf = ByteBuffer.allocate(thisBytes);
				noDataBuf.putInt(length);
				if (noDataValues != null) {
					for (final double noDataValue : noDataValues) {
						noDataBuf.putDouble(noDataValue);
					}
				}
				noDataValuesBytes.add(noDataBuf.array());
			}
			final ByteBuffer noDataBuf = ByteBuffer.allocate(totalBytes);
			noDataBuf.putInt(noDataValuesPerBand.length);
			for (final byte[] noDataValueBytes : noDataValuesBytes) {
				noDataBuf.put(noDataValueBytes);
			}
			return noDataBuf.array();
		}
		else {
			return new byte[] {};
		}
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
			final byte[] valueBinary = new byte[entryBinary.length - keyLength - 4];
			entryBuf.get(keyBinary);
			entryBuf.get(valueBinary);
			metadata.put(
					StringUtils.stringFromBinary(keyBinary),
					StringUtils.stringFromBinary(valueBinary));
		}
		final int histogramConfigLength = buf.getInt();
		if (histogramConfigLength == 0) {
			histogramConfig = null;
		}
		else {
			final byte[] histogramConfigBinary = new byte[histogramConfigLength];
			buf.get(histogramConfigBinary);
			histogramConfig = PersistenceUtils.fromBinary(
					histogramConfigBinary,
					HistogramConfig.class);
		}
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

		final int minsBinaryLength = buf.getInt();
		if (minsBinaryLength == 0) {
			minsPerBand = null;
		}
		else {
			minsPerBand = new double[minsBinaryLength / 8];
			for (int b = 0; b < minsPerBand.length; b++) {
				minsPerBand[b] = buf.getDouble();
			}
		}

		final int maxesBinaryLength = buf.getInt();
		if (maxesBinaryLength == 0) {
			maxesPerBand = null;
		}
		else {
			maxesPerBand = new double[maxesBinaryLength / 8];
			for (int b = 0; b < maxesPerBand.length; b++) {
				maxesPerBand[b] = buf.getDouble();
			}
		}

		final int namesLength = buf.getInt();
		if (namesLength == 0) {
			namesPerBand = null;
		}
		else {
			namesPerBand = new String[namesLength];
			for (int b = 0; b < namesPerBand.length; b++) {
				final int nameSize = buf.getInt();
				final byte[] nameBytes = new byte[nameSize];
				buf.get(nameBytes);
				namesPerBand[b] = StringUtils.stringFromBinary(nameBytes);
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
					RootMergeStrategy.class);
		}
		buildPyramid = (buf.get() != 0);
		equalizeHistogram = (buf.get() != 0);
		interpolation = Interpolation.getInstance(buf.get());
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
	public ByteArrayId[] getSupportedStatisticsTypes() {
		return supportedStatsTypes;
	}

	@Override
	public DataStatistics<GridCoverage> createDataStatistics(
			final ByteArrayId statisticsType ) {
		if (OverviewStatistics.STATS_TYPE.equals(statisticsType)) {
			return new OverviewStatistics(
					new ByteArrayId(
							coverageName));
		}
		else if (BoundingBoxDataStatistics.STATS_TYPE.equals(statisticsType)) {
			return new RasterBoundingBoxStatistics(
					new ByteArrayId(
							coverageName));
		}
		else if (RasterFootprintStatistics.STATS_TYPE.equals(statisticsType)) {
			return new RasterFootprintStatistics(
					new ByteArrayId(
							coverageName));
		}
		else if (HistogramStatistics.STATS_TYPE.equals(statisticsType) && (histogramConfig != null)) {
			return new HistogramStatistics(
					new ByteArrayId(
							coverageName),
					histogramConfig);
		}
		// HP Fortify "Log Forging" false positive
		// What Fortify considers "user input" comes only
		// from users with OS-level access anyway
		LOGGER.warn("Unrecognized statistics type " + statisticsType.getString() + " using count statistic");
		return new CountDataStatistics<GridCoverage>(
				getAdapterId(),
				statisticsType);
	}

	public double[][] getNoDataValuesPerBand() {
		return noDataValuesPerBand;
	}

	@Override
	public EntryVisibilityHandler<GridCoverage> getVisibilityHandler(
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
					// first attempt to retain the min and max with a "normal"
					// category
					!Double.isNaN(minimum) && !Double.isNaN(maximum) ? new Category[] {
						new Category(
								Vocabulary.formatInternational(VocabularyKeys.NORMAL),
								(Color) null,
								NumberRange.create(
										minimum,
										maximum)),
					} :
					// if that doesn't work, attempt to retain the nodata
					// category
							!Double.isNaN(nodata) ? new Category[] {
								new Category(
										Vocabulary.formatInternational(VocabularyKeys.NODATA),
										new Color(
												0,
												0,
												0,
												0),
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
		public InternationalString[] getCategoryNames()
				throws IllegalStateException {
			return new InternationalString[] {
				SimpleInternationalString.wrap("Background")
			};
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (!(obj instanceof SimplifiedGridSampleDimension)) {
				return false;
			}
			return super.equals(obj);
		}

		@Override
		public int hashCode() {
			return super.hashCode();
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

	public Map<String, String> getConfiguredOptions() {
		final Map<String, String> configuredOptions = new HashMap<String, String>();
		if (mergeStrategy != null) {
			final String mergeStrategyStr = ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(mergeStrategy));

			configuredOptions.put(
					RasterTileRowTransform.MERGE_STRATEGY_KEY,
					mergeStrategyStr);
		}
		return configuredOptions;
	}

	@Override
	public HadoopWritableSerializer<GridCoverage, GridCoverageWritable> createWritableSerializer() {
		return new HadoopWritableSerializer<GridCoverage, GridCoverageWritable>() {

			@Override
			public GridCoverageWritable toWritable(
					final GridCoverage entry ) {
				final Envelope env = entry.getEnvelope();
				final DataBuffer dataBuffer = entry.getRenderedImage().copyData(
						new InternalWritableRaster(
								sampleModel.createCompatibleSampleModel(
										tileSize,
										tileSize),
								new Point())).getDataBuffer();
				Persistable metadata = null;
				if (entry instanceof GridCoverage2D) {
					final Object metadataObj = ((GridCoverage2D) entry).getProperty(TILE_METADATA_PROPERTY_KEY);
					if ((metadataObj != null) && (metadataObj instanceof Persistable)) {
						metadata = (Persistable) metadataObj;
					}
				}
				return new GridCoverageWritable(
						new RasterTile(
								dataBuffer,
								metadata),
						env.getMinimum(0),
						env.getMaximum(0),
						env.getMinimum(1),
						env.getMaximum(1));
			}

			@Override
			public GridCoverage fromWritable(
					final GridCoverageWritable writable ) {
				final ReferencedEnvelope mapExtent = new ReferencedEnvelope(
						writable.getMinX(),
						writable.getMaxX(),
						writable.getMinY(),
						writable.getMaxY(),
						GeoWaveGTRasterFormat.DEFAULT_CRS);
				try {
					return prepareCoverage(
							writable.getRasterTile(),
							tileSize,
							mapExtent);
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to read raster data",
							e);
				}
				return null;
			}
		};
	}

	public boolean isEqualizeHistogram() {
		return equalizeHistogram;
	}

	public Interpolation getInterpolation() {
		return interpolation;
	}

	@Override
	public Map<String, String> getOptions(
			final Map<String, String> existingOptions ) {
		final Map<String, String> configuredOptions = getConfiguredOptions();
		if (existingOptions == null) {
			return configuredOptions;
		}
		final Map<String, String> mergedOptions = new HashMap<String, String>(
				configuredOptions);
		for (final Entry<String, String> e : existingOptions.entrySet()) {
			final String configuredValue = configuredOptions.get(e.getKey());
			if ((e.getValue() == null) && (configuredValue == null)) {
				continue;
			}
			else if ((e.getValue() == null) || ((e.getValue() != null) && !e.getValue().equals(
					configuredValue))) {
				final String newValue = mergeOption(
						e.getKey(),
						e.getValue(),
						configuredValue);
				if ((newValue != null) && newValue.equals(e.getValue())) {
					// once merged the value didn't
					// change, so just continue
					continue;
				}
				if (newValue == null) {
					mergedOptions.remove(e.getKey());
				}
				else {
					mergedOptions.put(
							e.getKey(),
							newValue);
				}
			}
		}
		for (final Entry<String, String> e : configuredOptions.entrySet()) {
			if (!existingOptions.containsKey(e.getKey())) {
				// existing value should be null
				// because this key is contained in
				// the merged set
				if (e.getValue() == null) {
					continue;
				}
				else {
					final String newValue = mergeOption(
							e.getKey(),
							null,
							e.getValue());
					if (newValue == null) {
						mergedOptions.remove(e.getKey());
					}
					else {
						mergedOptions.put(
								e.getKey(),
								newValue);
					}
				}
			}
		}
		return mergedOptions;
	}

	private String mergeOption(
			final String optionKey,
			final String currentValue,
			final String nextValue ) {
		if ((currentValue == null) || currentValue.trim().isEmpty()) {
			return nextValue;
		}
		else if ((nextValue == null) || nextValue.trim().isEmpty()) {
			return currentValue;
		}
		if (RasterTileRowTransform.MERGE_STRATEGY_KEY.equals(optionKey)) {
			final byte[] currentStrategyBytes = ByteArrayUtils.byteArrayFromString(currentValue);
			final byte[] nextStrategyBytes = ByteArrayUtils.byteArrayFromString(nextValue);
			final RootMergeStrategy currentStrategy = PersistenceUtils.fromBinary(
					currentStrategyBytes,
					RootMergeStrategy.class);
			final RootMergeStrategy nextStrategy = PersistenceUtils.fromBinary(
					nextStrategyBytes,
					RootMergeStrategy.class);
			currentStrategy.merge(nextStrategy);
			return ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(currentStrategy));
		}
		return nextValue;
	}

	@Override
	public RowTransform<RasterTile<?>> getTransform() {
		if (mergeStrategy != null) {
			return new RasterTileRowTransform();
		}
		else {
			return null;
		}
	}

	@Override
	public int getPositionOfOrderedField(
			final CommonIndexModel model,
			final ByteArrayId fieldId ) {
		int i = 0;
		for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
			if (fieldId.equals(dimensionField.getFieldId())) {
				return i;
			}
			i++;
		}
		if (fieldId.equals(DATA_FIELD_ID)) {
			return i;
		}
		return -1;
	}

	@Override
	public ByteArrayId getFieldIdForPosition(
			final CommonIndexModel model,
			final int position ) {
		if (position < model.getDimensions().length) {
			int i = 0;
			for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
				if (i == position) {
					return dimensionField.getFieldId();
				}
				i++;
			}
		}
		else {
			final int numDimensions = model.getDimensions().length;
			if (position == numDimensions) {
				return DATA_FIELD_ID;
			}
		}
		return null;
	}
}
