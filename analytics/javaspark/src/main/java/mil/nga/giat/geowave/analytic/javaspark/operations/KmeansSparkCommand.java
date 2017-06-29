package mil.nga.giat.geowave.analytic.javaspark.operations;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.util.Stopwatch;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.javaspark.KMeansHullGenerator;
import mil.nga.giat.geowave.analytic.javaspark.KMeansRunner;
import mil.nga.giat.geowave.analytic.mapreduce.operations.AnalyticSection;
import mil.nga.giat.geowave.analytic.mapreduce.operations.options.PropertyManagementConverter;
import mil.nga.giat.geowave.analytic.param.StoreParameters;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import scala.Tuple2;

@GeowaveOperation(name = "kmeansspark", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "KMeans Clustering via Spark ML")
public class KmeansSparkCommand extends
		DefaultOperation implements
		Command
{
	private final static Logger LOGGER = LoggerFactory.getLogger(KmeansSparkCommand.class);

	@Parameter(description = "<input storename> <output storename>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private KMeansSparkOptions kMeansSparkOptions = new KMeansSparkOptions();

	DataStorePluginOptions inputDataStore = null;
	DataStorePluginOptions outputDataStore = null;

	// Log some timing
	Stopwatch stopwatch = new Stopwatch();

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <input storename> <output storename>");
		}

		final String inputStoreName = parameters.get(0);
		final String outputStoreName = parameters.get(1);

		// Config file
		final File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		// Attempt to load stores.
		if (inputDataStore == null) {
			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find input store: " + inputStoreLoader.getStoreName());
			}
			inputDataStore = inputStoreLoader.getDataStorePlugin();
		}

		if (outputDataStore == null) {
			final StoreLoader outputStoreLoader = new StoreLoader(
					outputStoreName);
			if (!outputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find output store: " + outputStoreLoader.getStoreName());
			}
			outputDataStore = outputStoreLoader.getDataStorePlugin();
		}

		// Save a reference to the store in the property management.
		final PersistableStore persistedStore = new PersistableStore(
				inputDataStore);
		final PropertyManagement properties = new PropertyManagement();
		properties.store(
				StoreParameters.StoreParam.INPUT_STORE,
				persistedStore);

		// Convert properties from DBScanOptions and CommonOptions
		final PropertyManagementConverter converter = new PropertyManagementConverter(
				properties);
		converter.readProperties(kMeansSparkOptions);

		final KMeansRunner runner = new KMeansRunner();
		runner.setAppName(kMeansSparkOptions.getAppName());
		runner.setMaster(kMeansSparkOptions.getMaster());

		runner.setInputDataStore(inputDataStore);
		runner.setNumClusters(kMeansSparkOptions.getNumClusters());
		runner.setNumIterations(kMeansSparkOptions.getNumIterations());

		if (kMeansSparkOptions.getEpsilon() != null) {
			runner.setEpsilon(kMeansSparkOptions.getEpsilon());
		}

		stopwatch.reset();
		stopwatch.start();

		try {
			runner.run();

		}
		catch (final IOException e) {
			throw new RuntimeException(
					"Failed to execute: " + e.getMessage());
		}

		stopwatch.stop();

		final KMeansModel clusterModel = runner.getOutputModel();

		LOGGER.warn("KMeans runner took " + stopwatch.getTimeString());

		// output cluster centroids (and hulls) to output datastore
		writeClusterCentroids(clusterModel);

		if (kMeansSparkOptions.isGenerateHulls()) {
			final JavaRDD<Vector> inputCentroids = runner.getInputCentroids();
			generateHulls(
					inputCentroids,
					clusterModel);
		}
	}

	private void writeClusterCentroids(
			final KMeansModel clusterModel ) {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("kmeans-centroids");
		typeBuilder.setNamespaceURI(BasicFeatureTypes.DEFAULT_NAMESPACE);

		try {
			typeBuilder.setCRS(CRS.decode(
					"EPSG:4326",
					true));
		}
		catch (final FactoryException fex) {
			LOGGER.error(
					fex.getMessage(),
					fex);
		}

		final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();

		typeBuilder.add(attrBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				Geometry.class.getName().toString()));

		typeBuilder.add(attrBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"KMeansData"));

		final SimpleFeatureType sfType = typeBuilder.buildFeatureType();
		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);

		final FeatureDataAdapter featureAdapter = new FeatureDataAdapter(
				sfType);

		final DataStore featureStore = outputDataStore.createDataStore();
		final PrimaryIndex featureIndex = new SpatialDimensionalityTypeProvider().createPrimaryIndex();

		try (IndexWriter writer = featureStore.createWriter(
				featureAdapter,
				featureIndex)) {
			int i = 0;
			for (final Vector center : clusterModel.clusterCenters()) {
				final double lon = center.apply(0);
				final double lat = center.apply(1);

				sfBuilder.set(
						Geometry.class.getName(),
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								lon,
								lat)));

				sfBuilder.set(
						"KMeansData",
						"KMeansCentroid");

				final SimpleFeature sf = sfBuilder.buildFeature("Centroid-" + i++);

				writer.write(sf);
			}
		}
		catch (final MismatchedIndexToAdapterMapping e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}
	}

	private void generateHulls(
			final JavaRDD<Vector> inputCentroids,
			final KMeansModel clusterModel ) {
		stopwatch.reset();
		stopwatch.start();

		final JavaPairRDD<Integer, Geometry> hullRdd = KMeansHullGenerator.generateHullsRDD(
				inputCentroids,
				clusterModel);

		stopwatch.stop();
		LOGGER.warn("KMeansHullGenerator took " + stopwatch.getTimeString() + " for " + inputCentroids.count()
				+ " points");

		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("kmeans-hulls");
		typeBuilder.setNamespaceURI(BasicFeatureTypes.DEFAULT_NAMESPACE);
		try {
			typeBuilder.setCRS(CRS.decode(
					"EPSG:4326",
					true));
		}
		catch (final FactoryException e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}

		final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();

		typeBuilder.add(attrBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				Geometry.class.getName().toString()));

		typeBuilder.add(attrBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"KMeansData"));

		final SimpleFeatureType sfType = typeBuilder.buildFeatureType();
		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);

		final FeatureDataAdapter featureAdapter = new FeatureDataAdapter(
				sfType);

		final DataStore featureStore = outputDataStore.createDataStore();
		final PrimaryIndex featureIndex = new SpatialDimensionalityTypeProvider().createPrimaryIndex();

		try (IndexWriter writer = featureStore.createWriter(
				featureAdapter,
				featureIndex)) {

			int i = 0;
			for (final Tuple2<Integer, Geometry> hull : hullRdd.collect()) {
				sfBuilder.set(
						Geometry.class.getName(),
						hull._2);

				sfBuilder.set(
						"KMeansData",
						"KMeansHull");

				final SimpleFeature sf = sfBuilder.buildFeature("Hull-" + i++);

				writer.write(sf);
			}
		}
		catch (final MismatchedIndexToAdapterMapping e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String storeName ) {
		parameters = new ArrayList<String>();
		parameters.add(storeName);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputDataStore;
	}

	public void setInputStoreOptions(
			final DataStorePluginOptions inputStoreOptions ) {
		inputDataStore = inputStoreOptions;
	}

	public DataStorePluginOptions getOutputStoreOptions() {
		return outputDataStore;
	}

	public void setOutputStoreOptions(
			final DataStorePluginOptions outputStoreOptions ) {
		outputDataStore = outputStoreOptions;
	}

	public KMeansSparkOptions getKMeansSparkOptions() {
		return kMeansSparkOptions;
	}

	public void setKMeansSparkOptions(
			final KMeansSparkOptions kMeansSparkOptions ) {
		this.kMeansSparkOptions = kMeansSparkOptions;
	}
}
