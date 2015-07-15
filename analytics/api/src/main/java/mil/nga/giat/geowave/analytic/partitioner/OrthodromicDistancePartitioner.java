package mil.nga.giat.geowave.analytic.partitioner;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.measure.quantity.Length;
import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.GeometryCalculations;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;
import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureGeometryExtractor;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

/*
 * Calculates distance use orthodromic distance to calculate the bounding box around each
 * point.
 * 
 * The approach is slow and more accurate, resulting in more partitions of smaller size.
 * The class requires {@link CoordinateReferenceSystem} for the distance calculation
 * and {@link DimensionExtractor} to extract geometries and other dimensions.
 * 
 * The order of distances provided must match the order or dimensions extracted from the dimension
 * extractor.
 */
public class OrthodromicDistancePartitioner<T> extends
		AbstractPartitioner<T> implements
		Partitioner<T>
{
	final static Logger LOGGER = LoggerFactory.getLogger(OrthodromicDistancePartitioner.class);

	private Unit<Length> geometricDistanceUnit = SI.METER;
	private CoordinateReferenceSystem crs = null;
	private GeometryCalculations calculator;
	private DimensionExtractor<T> dimensionExtractor;
	private int latDimensionPosition;
	private int longDimensionPosition;

	public OrthodromicDistancePartitioner() {}

	public OrthodromicDistancePartitioner(
			final CoordinateReferenceSystem crs,
			final CommonIndexModel indexModel,
			final DimensionExtractor<T> dimensionExtractor,
			final double[] distancePerDimension,
			final Unit<Length> geometricDistanceUnit ) {
		super(
				distancePerDimension);
		this.crs = crs;
		this.geometricDistanceUnit = geometricDistanceUnit;
		this.dimensionExtractor = dimensionExtractor;
		initCalculator();
		initIndex(
				indexModel,
				distancePerDimension);
	}

	@Override
	protected NumericDataHolder getNumericData(
			final T entry ) {
		final NumericDataHolder numericDataHolder = new NumericDataHolder();

		final Geometry entryGeometry = dimensionExtractor.getGeometry(entry);
		final double otherDimensionData[] = dimensionExtractor.getDimensions(entry);
		numericDataHolder.primary = getNumericData(
				entryGeometry.getEnvelope(),
				otherDimensionData);
		final List<Geometry> geometries = getGeometries(
				entryGeometry.getCentroid().getCoordinate(),
				getDistancePerDimension());
		final MultiDimensionalNumericData[] values = new MultiDimensionalNumericData[geometries.size()];
		int i = 0;
		for (final Geometry geometry : geometries) {
			values[i++] = getNumericData(
					geometry.getEnvelope(),
					otherDimensionData);
		}
		numericDataHolder.expansion = values;
		return numericDataHolder;
	}

	private MultiDimensionalNumericData getNumericData(
			final Geometry geometry,
			final double[] otherDimensionData ) {
		final DimensionField<?>[] dimensionFields = getIndex().getIndexModel().getDimensions();
		final NumericData[] numericData = new NumericData[dimensionFields.length];
		final double[] distancePerDimension = getDistancePerDimension();
		int otherIndex = 0;

		for (int i = 0; i < dimensionFields.length; i++) {
			final double minValue = (i == this.longDimensionPosition) ? geometry.getEnvelopeInternal().getMinX() : (i == this.latDimensionPosition ? geometry.getEnvelopeInternal().getMinY() : otherDimensionData[otherIndex] - distancePerDimension[i]);
			final double maxValue = (i == this.longDimensionPosition) ? geometry.getEnvelopeInternal().getMaxX() : (i == this.latDimensionPosition ? geometry.getEnvelopeInternal().getMaxY() : otherDimensionData[otherIndex] + distancePerDimension[i]);
			if ((i != this.longDimensionPosition) && (i != latDimensionPosition)) {
				otherIndex++;
			}
			numericData[i] = new NumericRange(
					minValue,
					maxValue);
		}
		return new BasicNumericDataset(
				numericData);
	}

	private static int findLongitude(
			final CommonIndexModel indexModel ) {
		return indexOf(
				indexModel.getDimensions(),
				LongitudeDefinition.class);
	}

	private static int findLatitude(
			final CommonIndexModel indexModel ) {
		return indexOf(
				indexModel.getDimensions(),
				LatitudeDefinition.class);
	}

	private static int indexOf(
			final DimensionField<?> fields[],
			final Class<? extends NumericDimensionDefinition> clazz ) {

		for (int i = 0; i < fields.length; i++) {
			if (clazz.isInstance(fields[i].getBaseDefinition())) {
				return i;
			}
		}
		return -1;
	}

	private List<Geometry> getGeometries(
			final Coordinate coordinate,
			final double[] distancePerDimension ) {
		return calculator.buildSurroundingGeometries(
				new double[] {
					distancePerDimension[longDimensionPosition],
					distancePerDimension[latDimensionPosition]
				},
				geometricDistanceUnit == null ? SI.METER : geometricDistanceUnit,
				coordinate);
	}

	private void initCalculator() {
		// this block would only occur in test or in failed initialization
		if (crs == null) {
			try {
				crs = CRS.decode(
						"EPSG:4326",
						true);
			}
			catch (final FactoryException e) {
				LOGGER.error(
						"CRS not providd and default EPSG:4326 cannot be instantiated",
						e);
				throw new RuntimeException(
						e);
			}
		}

		calculator = new GeometryCalculations(
				crs);
	}

	@Override
	protected void initIndex(
			final CommonIndexModel indexModel,
			final double[] distancePerDimension ) {

		longDimensionPosition = findLongitude(indexModel);
		latDimensionPosition = findLatitude(indexModel);

		final List<Geometry> geos = getGeometries(
				new Coordinate(
						0,
						0),
				distancePerDimension);

		final Envelope envelope = geos.get(
				0).getEnvelopeInternal();

		// set up the distances based on geometry (orthodromic distance)
		final double[] distancePerDimensionForIndex = new double[distancePerDimension.length];
		for (int i = 0; i < distancePerDimension.length; i++) {
			distancePerDimensionForIndex[i] = (i == longDimensionPosition) ? envelope.getWidth() / 2.0 : (i == latDimensionPosition ? envelope.getHeight() / 2.0 : distancePerDimension[i]);
			LOGGER.info(
					"Dimension size {} is {} ",
					i,
					distancePerDimensionForIndex[i]);
		}

		super.initIndex(
				indexModel,
				distancePerDimensionForIndex);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void initialize(
			final ConfigurationWrapper context )
			throws IOException {

		final String crsName = context.getString(
				GlobalParameters.Global.CRS_ID,
				this.getClass(),
				"EPSG:4326");
		try {
			crs = CRS.decode(
					crsName,
					true);
		}
		catch (final FactoryException e) {
			throw new IOException(
					"Cannot find CRS " + crsName,
					e);
		}

		try {
			dimensionExtractor = context.getInstance(
					ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS,
					this.getClass(),
					DimensionExtractor.class,
					SimpleFeatureGeometryExtractor.class);
		}
		catch (final Exception ex) {
			throw new IOException(
					"Cannot find class for  " + ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS.toString(),
					ex);
		}

		final String distanceUnit = context.getString(
				ClusteringParameters.Clustering.GEOMETRIC_DISTANCE_UNIT,
				this.getClass(),
				"m");

		this.geometricDistanceUnit = (Unit<Length>) Unit.valueOf(distanceUnit);

		initCalculator();

		super.initialize(context);
	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		super.fillOptions(options);
		ClusteringParameters.fillOptions(
				options,
				new ClusteringParameters.Clustering[] {
					ClusteringParameters.Clustering.GEOMETRIC_DISTANCE_UNIT
				});
		ExtractParameters.fillOptions(
				options,
				new ExtractParameters.Extract[] {
					ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS
				});

	}

	@Override
	public void setup(
			final PropertyManagement runTimeProperties,
			final Configuration configuration ) {
		super.setup(
				runTimeProperties,
				configuration);
		RunnerUtils.setParameter(
				configuration,
				getClass(),
				runTimeProperties,
				new ParameterEnum[] {
					GlobalParameters.Global.CRS_ID,
					ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS,
					ClusteringParameters.Clustering.GEOMETRIC_DISTANCE_UNIT
				});
	}
}
