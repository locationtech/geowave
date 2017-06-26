package mil.nga.giat.geowave.analytic.javaspark.operations;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

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
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "kmeansspark", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "KMeans Clustering via Spark ML")
public class KmeansSparkCommand extends
		DefaultOperation implements
		Command
{
	@Parameter(description = "<input storename> <output storename>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private KMeansSparkOptions kMeansSparkOptions = new KMeansSparkOptions();

	DataStorePluginOptions inputDataStore = null;
	DataStorePluginOptions outputDataStore = null;

	@Override
	public void execute(
			OperationParams params )
			throws Exception {

		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <input storename> <output storename>");
		}

		String inputStoreName = parameters.get(0);
		String outputStoreName = parameters.get(1);

		// Config file
		File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		// Attempt to load stores.
		if (inputDataStore == null) {
			StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find input store: " + inputStoreLoader.getStoreName());
			}
			inputDataStore = inputStoreLoader.getDataStorePlugin();
		}

		if (outputDataStore == null) {
			StoreLoader outputStoreLoader = new StoreLoader(
					outputStoreName);
			if (!outputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find output store: " + outputStoreLoader.getStoreName());
			}
			outputDataStore = outputStoreLoader.getDataStorePlugin();
		}

		// Save a reference to the store in the property management.
		PersistableStore persistedStore = new PersistableStore(
				inputDataStore);
		final PropertyManagement properties = new PropertyManagement();
		properties.store(
				StoreParameters.StoreParam.INPUT_STORE,
				persistedStore);

		// Convert properties from DBScanOptions and CommonOptions
		PropertyManagementConverter converter = new PropertyManagementConverter(
				properties);
		converter.readProperties(kMeansSparkOptions);

		KMeansRunner runner = new KMeansRunner(
				kMeansSparkOptions.getAppName(),
				kMeansSparkOptions.getMaster());

		runner.setInputDataStore(inputDataStore);
		runner.setNumClusters(kMeansSparkOptions.getNumClusters());
		runner.setNumIterations(kMeansSparkOptions.getNumIterations());

		if (kMeansSparkOptions.getEpsilon() != null) {
			runner.setEpsilon(kMeansSparkOptions.getEpsilon());
		}

		try {
			runner.run();
		}
		catch (IOException e) {
			throw new RuntimeException(
					"Failed to execute: " + e.getMessage());
		}

		KMeansModel clusterModel = runner.getOutputModel();

		// output cluster centroids (and hulls) to output datastore
		writeClusterCentroids(clusterModel);

		if (kMeansSparkOptions.isGenerateHulls()) {
			JavaRDD<Vector> inputCentroids = runner.getInputCentroids();
			generateHulls(
					inputCentroids,
					clusterModel);
		}
	}

	private void writeClusterCentroids(
			KMeansModel clusterModel ) {
		final SimpleFeatureTypeBuilder typebuilder = new SimpleFeatureTypeBuilder();
		typebuilder.setName("kmeans-centroids");

		final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();

		typebuilder.add(attrBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				Geometry.class.getName().toString()));

		typebuilder.add(attrBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"KMeansData"));

		final SimpleFeatureType sfType = typebuilder.buildFeatureType();
		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);

		final FeatureDataAdapter featureAdapter = new FeatureDataAdapter(
				sfType);

		DataStore featureStore = outputDataStore.createDataStore();

		PrimaryIndex featureIndex = new SpatialDimensionalityTypeProvider().createPrimaryIndex();

		try {
			IndexWriter writer = featureStore.createWriter(
					featureAdapter,
					featureIndex);

			int i = 0;
			for (Vector center : clusterModel.clusterCenters()) {
				double lon = center.apply(0);
				double lat = center.apply(1);

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
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void generateHulls(
			JavaRDD<Vector> inputCentroids,
			KMeansModel clusterModel ) {
		Geometry[] hulls = KMeansHullGenerator.generateHulls(
				inputCentroids,
				clusterModel);

		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("kmeans-hulls");

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

		DataStore featureStore = outputDataStore.createDataStore();

		PrimaryIndex featureIndex = new SpatialDimensionalityTypeProvider().createPrimaryIndex();

		try {
			IndexWriter writer = featureStore.createWriter(
					featureAdapter,
					featureIndex);

			int i = 0;
			for (Geometry hull : hulls) {

				sfBuilder.set(
						Geometry.class.getName(),
						hull);

				sfBuilder.set(
						"KMeansData",
						"KMeansHull");

				final SimpleFeature sf = sfBuilder.buildFeature("Hull-" + i++);

				writer.write(sf);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String storeName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputDataStore;
	}

	public void setInputStoreOptions(
			DataStorePluginOptions inputStoreOptions ) {
		this.inputDataStore = inputStoreOptions;
	}

	public DataStorePluginOptions getOutputStoreOptions() {
		return outputDataStore;
	}

	public void setOutputStoreOptions(
			DataStorePluginOptions outputStoreOptions ) {
		this.outputDataStore = outputStoreOptions;
	}

	public KMeansSparkOptions getKMeansSparkOptions() {
		return kMeansSparkOptions;
	}

	public void setKMeansSparkOptions(
			KMeansSparkOptions kMeansSparkOptions ) {
		this.kMeansSparkOptions = kMeansSparkOptions;
	}
}
