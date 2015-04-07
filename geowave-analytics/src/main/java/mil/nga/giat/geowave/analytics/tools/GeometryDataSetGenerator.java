package mil.nga.giat.geowave.analytics.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.analytics.distance.DistanceFn;
import mil.nga.giat.geowave.analytics.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.GeometryType;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Generate clusters of geometries.
 * 
 */
public class GeometryDataSetGenerator
{
	final static Logger LOGGER = LoggerFactory.getLogger(GeometryDataSetGenerator.class);
	private final Random rand = new Random();
	private final GeometryFactory geoFactory = new GeometryFactory();
	private final DistanceFn<SimpleFeature> distanceFunction;
	private final SimpleFeatureBuilder builder;
	// coordinate system boundaries
	private SimpleFeature minFeature;
	private double[] minAxis;
	private double[] maxAxis;
	private CoordinateSystem coordSystem;

	public GeometryDataSetGenerator(
			final DistanceFn<SimpleFeature> distanceFunction,
			final SimpleFeatureBuilder builder ) {
		super();
		this.distanceFunction = distanceFunction;
		this.builder = builder;
		init();
	}

	public SimpleFeature getCorner() {
		return minFeature;
	}

	public Geometry getBoundingRegion() {
		final int adder[] = {
			1,
			2,
			-1,
			2
		};
		int num = 0;
		int addCnt = 0;
		final int dims = coordSystem.getDimension();
		final int coords = (int) Math.pow(
				dims,
				2);

		final Coordinate[] coordinates = new Coordinate[coords + 1];
		for (int i = 0; i < coords; i++) {
			coordinates[i] = new Coordinate();
			for (int j = 0; j < dims; j++) {
				final boolean isMin = ((num >> j) % 2) == 0;
				coordinates[i].setOrdinate(
						j,
						isMin ? minAxis[j] : maxAxis[j]);
			}
			num += adder[addCnt];
			addCnt = (addCnt + 1) % 4;
		}
		coordinates[coords] = coordinates[0];
		return geoFactory.createPolygon(coordinates);
	}

	/**
	 * Calculate the range for the given bounds
	 * 
	 * @param factor
	 * @param minAxis
	 * @param maxAxis
	 * @return
	 */
	private double[] createRange(
			final double factor,
			final double[] minAxis,
			final double[] maxAxis ) {
		final double range[] = new double[minAxis.length];
		for (int i = 0; i < minAxis.length; i++) {
			range[i] = (maxAxis[i] - minAxis[i]) * factor;
		}
		return range;
	}

	/**
	 * Pick a random grid cell and supply the boundary. The grid is determined
	 * by the parameter,which provides a percentage of distance over the total
	 * range for each cell.
	 * 
	 * @param minCenterDistanceFactor
	 * @return
	 */
	private Pair<double[], double[]> gridCellBounds(
			final double minCenterDistanceFactor,
			final double[] minAxis,
			final double[] maxAxis ) {
		final double range[] = createRange(
				1.0,
				minAxis,
				maxAxis);
		final double min[] = new double[range.length];
		final double max[] = new double[range.length];
		for (int i = 0; i < range.length; i++) {
			min[i] = Math.max(
					minAxis[i] + (minCenterDistanceFactor * (rand.nextInt(Integer.MAX_VALUE) % (range[i] / minCenterDistanceFactor))),
					minAxis[i]);
			max[i] = Math.min(
					min[i] + (minCenterDistanceFactor * range[i]),
					maxAxis[i]);
		}
		return Pair.of(
				min,
				max);
	}

	public void writeToGeoWave(
			final String zookeeper,
			final String instance,
			final String user,
			final String password,
			final String namespace,
			final List<SimpleFeature> featureData )
			throws IOException {
		final Instance zookeeperInstance = new ZooKeeperInstance(
				instance,
				zookeeper);
		try {
			final Connector accumuloConnector = zookeeperInstance.getConnector(
					user,
					new PasswordToken(
							password));

			final DataStore dataStore = new AccumuloDataStore(
					new BasicAccumuloOperations(
							accumuloConnector,
							namespace));
			final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
			final FeatureDataAdapter adapter = new FeatureDataAdapter(
					featureData.get(
							0).getFeatureType());
			final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(
					featureData.get(
							0).getFeatureType());

			LOGGER.info("Accumulo " + zookeeper + " instance " + instance + " namespace " + namespace);
			LOGGER.info("Writing " + featureData.size() + " records to " + adapter.getType().getTypeName());
			Integer idCounter = 0;
			for (final SimpleFeature feature : featureData) {

				dataStore.ingest(
						adapter,
						index,
						feature);
				featureBuilder.reset();

				idCounter++;
			}

		}
		catch (AccumuloException | AccumuloSecurityException e) {
			throw new IOException(
					"Cannot write to " + instance,
					e);
		}
	}

	public List<SimpleFeature> generatePointSet(
			final double minCenterDistanceFactor,
			final double outlierFactor,
			final int numberOfCenters,
			final int minSetSize ) {
		return this.generatePointSet(
				minCenterDistanceFactor,
				outlierFactor,
				numberOfCenters,
				minSetSize,
				minAxis,
				maxAxis);
	}

	public List<SimpleFeature> generatePointSet(
			final double minCenterDistanceFactor,
			final double outlierFactor,
			final int numberOfCenters,
			final int minSetSize,
			final double minAxis[],
			final double maxAxis[] ) {

		final List<SimpleFeature> pointSet = new ArrayList<SimpleFeature>();
		final List<double[]> minForCenter = new ArrayList<double[]>();
		final List<double[]> maxForCenter = new ArrayList<double[]>();
		final double range[] = createRange(
				minCenterDistanceFactor,
				minAxis,
				maxAxis);
		if (numberOfCenters >= minSetSize) {
			LOGGER.error("The number of centers passed much be less than the minimum set size");
			throw new IllegalArgumentException(
					"The number of centers passed much be less than the minimum set size");
		}

		final double minDistance = computeMinDistance(
				minCenterDistanceFactor,
				minAxis,
				maxAxis);

		/**
		 * Pick the initial centers which have minimum distance from each other.
		 * 
		 */
		while (pointSet.size() < numberOfCenters) {

			final Pair<double[], double[]> axis = gridCellBounds(
					minCenterDistanceFactor,
					minAxis,
					maxAxis);

			final SimpleFeature nextFeature = createNewFeature(
					axis.getLeft(),
					axis.getRight());
			if (isFarEnough(
					nextFeature,
					pointSet,
					minDistance)) {
				pointSet.add(nextFeature);
			}
		}

		/**
		 * Calculate the boundaries around each center point to place additional
		 * points, thus creating clusters
		 */
		for (final SimpleFeature center : pointSet) {
			final double[] centerMinAxis = new double[coordSystem.getDimension()];
			final double[] centerMaxAxis = new double[coordSystem.getDimension()];
			final Geometry geo = (Geometry) center.getDefaultGeometry();
			final Coordinate centerCoord = geo.getCentroid().getCoordinate();
			for (int i = 0; i < centerMinAxis.length; i++) {
				centerMinAxis[i] = centerCoord.getOrdinate(i) - (range[i] / 2.0);
				centerMaxAxis[i] = centerCoord.getOrdinate(i) + (range[i] / 2.0);
			}
			minForCenter.add(centerMinAxis);
			maxForCenter.add(centerMaxAxis);
		}

		/*
		 * Pick a random center point and add a new geometry with the bounding
		 * range around that point.
		 */
		final int clusterdItemsCount = (int) Math.ceil((minSetSize) * (1.0 - outlierFactor));
		while (pointSet.size() < clusterdItemsCount) {
			final int centerPos = rand.nextInt(Integer.MAX_VALUE) % minForCenter.size();

			pointSet.add(createNewFeature(
					minForCenter.get(centerPos),
					maxForCenter.get(centerPos)));
		}

		/**
		 * Add random points as potential outliers (no guarantees)
		 */
		while (pointSet.size() < minSetSize) {
			pointSet.add(createNewFeature(
					minAxis,
					maxAxis));
		}
		return pointSet;
	}

	public List<SimpleFeature> addRandomNoisePoints(
			List<SimpleFeature> pointSet,
			final int minSetSize,
			final double minAxis[],
			final double maxAxis[] ) {
		while (pointSet.size() < minSetSize) {
			pointSet.add(createNewFeature(
					minAxis,
					maxAxis));
		}
		return pointSet;
	}

	private void init() {
		final GeometryDescriptor geoDescriptor = builder.getFeatureType().getGeometryDescriptor();
		coordSystem = geoDescriptor.getType().getCoordinateReferenceSystem().getCoordinateSystem();

		minAxis = new double[coordSystem.getDimension()];
		maxAxis = new double[coordSystem.getDimension()];
		for (int i = 0; i < coordSystem.getDimension(); i++) {
			final CoordinateSystemAxis axis = coordSystem.getAxis(i);
			minAxis[i] = axis.getMinimumValue();
			maxAxis[i] = axis.getMaximumValue();
		}
		final int dims = coordSystem.getDimension();

		final Coordinate coordinate = new Coordinate();
		for (int i = 0; i < dims; i++) {
			coordinate.setOrdinate(
					i,
					minAxis[i]);
		}
		minFeature = createFeatureWithGeometry(geoFactory.createPoint(coordinate));

	}

	private boolean isFarEnough(
			final SimpleFeature feature,
			final List<SimpleFeature> set,
			final double minDistance ) {
		for (final SimpleFeature setItem : set) {
			if (distanceFunction.measure(
					feature,
					setItem) < minDistance) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Find the distance maximum distance of the entire space and multiply that
	 * by the distance factor to determine a minimum distance each initial
	 * center point occurs from each other.
	 * 
	 * @param minCenterDistanceFactor
	 * @return
	 */
	private double computeMinDistance(
			final double minCenterDistanceFactor,
			final double minAxis[],
			final double maxAxis[] ) {
		assert (minCenterDistanceFactor > 0.001);
		assert (minCenterDistanceFactor < 0.75);

		final int dims = coordSystem.getDimension();

		Coordinate coordinate = new Coordinate();
		for (int i = 0; i < dims; i++) {
			coordinate.setOrdinate(
					i,
					minAxis[i]);
		}
		final SimpleFeature minFeature = createFeatureWithGeometry(geoFactory.createPoint(coordinate));

		coordinate = new Coordinate();
		for (int i = 0; i < dims; i++) {
			coordinate.setOrdinate(
					i,
					maxAxis[i]);
		}

		final SimpleFeature maxFeature = createFeatureWithGeometry(geoFactory.createPoint(coordinate));

		return minCenterDistanceFactor * distanceFunction.measure(
				minFeature,
				maxFeature);
	}

	private SimpleFeature createNewFeature(
			final double minAxis[],
			final double maxAxis[] ) {

		final int dims = coordSystem.getDimension();

		final int shapeSize = (rand.nextInt(Integer.MAX_VALUE) % 5) + 1;
		final Coordinate[] shape = new Coordinate[shapeSize > 2 ? shapeSize + 1 : shapeSize];
		final double[] constrainedMaxAxis = Arrays.copyOf(
				maxAxis,
				maxAxis.length);
		final double[] constrainedMinAxis = Arrays.copyOf(
				minAxis,
				minAxis.length);
		for (int s = 0; s < shapeSize; s++) {
			final Coordinate coordinate = new Coordinate();
			for (int i = 0; i < dims; i++) {
				coordinate.setOrdinate(
						i,
						constrainedMinAxis[i] + (rand.nextDouble() * (constrainedMaxAxis[i] - constrainedMinAxis[i])));
			}
			shape[s] = coordinate;
			if (s == 0) {
				constrain(
						coordinate,
						constrainedMaxAxis,
						constrainedMinAxis);
			}
		}
		if (shapeSize > 2) {
			shape[shapeSize] = shape[0];
			return createFeatureWithGeometry(geoFactory.createLinearRing(
					shape).convexHull());
		}
		else if (shapeSize == 2) {
			return createFeatureWithGeometry(geoFactory.createLineString(shape));
		}
		else {
			return createFeatureWithGeometry(geoFactory.createPoint(shape[0]));
		}
	}

	/**
	 * Change the constrain min and max to center around the coordinate to keep
	 * the polygons tight.
	 * 
	 * @param coordinate
	 * @param constrainedMaxAxis
	 * @param constrainedMinAxis
	 */
	private void constrain(
			final Coordinate coordinate,
			final double[] constrainedMaxAxis,
			final double[] constrainedMinAxis ) {
		for (int i = 0; i < constrainedMaxAxis.length; i++) {
			final double range = (constrainedMaxAxis[i] - constrainedMinAxis[i]) * 0.001;
			constrainedMaxAxis[i] = Math.min(
					coordinate.getOrdinate(i) + range,
					constrainedMaxAxis[i]);
			constrainedMinAxis[i] = Math.max(
					coordinate.getOrdinate(i) - range,
					constrainedMinAxis[i]);
		}
	}

	private SimpleFeature createFeatureWithGeometry(
			final Geometry geometry ) {
		final Object values[] = new Object[builder.getFeatureType().getAttributeCount()];
		for (int i = 0; i < values.length; i++) {
			final AttributeDescriptor desc = builder.getFeatureType().getDescriptor(
					i);
			if (desc.getType() instanceof GeometryType) {
				values[i] = geometry;
			}
			else {
				final Class<?> binding = desc.getType().getBinding();
				if (String.class.isAssignableFrom(binding)) {
					values[i] = UUID.randomUUID().toString();
				}
			}
		}
		return builder.buildFeature(
				UUID.randomUUID().toString(),
				values);

	}

	public static void main(
			final String args[] )
			throws IOException {
		final GeometryDataSetGenerator dataGenerator = new GeometryDataSetGenerator(
				new FeatureCentroidDistanceFn(),
				getBuilder(args[5]));
		final String zookeeper = args[0].trim();
		final String instance = args[1].trim();
		final String user = args[2];
		final String password = args[3];
		final String namespace = args[4].trim();
		dataGenerator.writeToGeoWave(
				zookeeper,
				instance,
				user,
				password,
				namespace,
				dataGenerator.generatePointSet(
						0.2,
						0.2,
						5,
						5000,
						new double[] {
							-100,
							-45
						},
						new double[] {
							-90,
							-35
						}));
		dataGenerator.writeToGeoWave(
				zookeeper,
				instance,
				user,
				password,
				namespace,
				dataGenerator.generatePointSet(
						0.2,
						0.2,
						7,
						5000,
						new double[] {
							0,
							0
						},
						new double[] {
							10,
							10
						}));
		dataGenerator.writeToGeoWave(
				zookeeper,
				instance,
				user,
				password,
				namespace,
				dataGenerator.addRandomNoisePoints(
						dataGenerator.generatePointSet(
								0.2,
								0.2,
								6,
								5000,
								new double[] {
									65,
									35
								},
								new double[] {
									75,
									45
								}),
						6000,
						new double[] {
							-90,
							-90
						},
						new double[] {
							90,
							90
						}));
	}

	private static SimpleFeatureBuilder getBuilder(
			String name ) {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName(name);
		typeBuilder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate
														// reference
		// add attributes in order
		typeBuilder.add(
				"geom",
				Geometry.class);
		typeBuilder.add(
				"name",
				String.class);
		typeBuilder.add(
				"count",
				Long.class);

		// build the type
		return new SimpleFeatureBuilder(
				typeBuilder.buildFeatureType());
	}
}
