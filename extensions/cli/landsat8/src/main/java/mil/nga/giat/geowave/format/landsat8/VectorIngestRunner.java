package mil.nga.giat.geowave.format.landsat8;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

public class VectorIngestRunner extends
		AnalyzeRunner
{

	private final static Logger LOGGER = LoggerFactory.getLogger(RasterIngestRunner.class);
	protected final List<String> parameters;
	private IndexWriter bandWriter;
	private IndexWriter sceneWriter;

	private SimpleFeatureType sceneType;

	public VectorIngestRunner(
			final Landsat8BasicCommandLineOptions analyzeOptions,
			final List<String> parameters ) {
		super(
				analyzeOptions);
		this.parameters = parameters;
	}

	@Override
	protected void runInternal(
			final OperationParams params )
			throws Exception {
		try {
			// Ensure we have all the required arguments
			if (parameters.size() != 2) {
				throw new ParameterException(
						"Requires arguments: <storename> <comma delimited index/group list>");
			}
			final String inputStoreName = parameters.get(0);
			final String indexList = parameters.get(1);

			// Config file
			final File configFile = (File) params.getContext().get(
					ConfigOptions.PROPERTIES_FILE_CONTEXT);

			// Attempt to load input store.
			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			final DataStorePluginOptions storeOptions = inputStoreLoader.getDataStorePlugin();
			final DataStore store = storeOptions.createDataStore();

			// Load the Indices
			final IndexLoader indexLoader = new IndexLoader(
					indexList);
			if (!indexLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find index(s) by name: " + indexList);
			}
			final List<IndexPluginOptions> indexOptions = indexLoader.getLoadedIndexes();

			final PrimaryIndex[] indices = new PrimaryIndex[indexOptions.size()];
			int i = 0;
			for (final IndexPluginOptions dimensionType : indexOptions) {
				final PrimaryIndex primaryIndex = dimensionType.createPrimaryIndex();
				if (primaryIndex == null) {
					LOGGER.error("Could not get index instance, getIndex() returned null;");
					throw new IOException(
							"Could not get index instance, getIndex() returned null");
				}
				indices[i++] = primaryIndex;
			}
			sceneType = SceneFeatureIterator.createFeatureType();
			final FeatureDataAdapter sceneAdapter = new FeatureDataAdapter(
					sceneType);
			sceneWriter = store.createWriter(
					sceneAdapter,
					indices);
			final SimpleFeatureType bandType = BandFeatureIterator.createFeatureType(sceneType);
			final FeatureDataAdapter bandAdapter = new FeatureDataAdapter(
					bandType);
			bandWriter = store.createWriter(
					bandAdapter,
					indices);
			super.runInternal(params);
		}
		finally {
			if (sceneWriter != null) {
				try {
					sceneWriter.close();
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to close writer for scene vectors",
							e);
				}
			}
			if (bandWriter != null) {
				try {
					bandWriter.close();
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to close writer for band vectors",
							e);
				}
			}
		}
	}

	@Override
	protected void nextBand(
			final SimpleFeature band,
			final AnalysisInfo analysisInfo ) {
		try {
			bandWriter.write(band);
		}
		catch (IOException e) {
			LOGGER.error(
					"Unable to write next band",
					e);
		}
		super.nextBand(
				band,
				analysisInfo);
	}

	@Override
	protected void nextScene(
			final SimpleFeature firstBandOfScene,
			final AnalysisInfo analysisInfo ) {
		writeScene(
				sceneType,
				firstBandOfScene,
				sceneWriter);
		super.nextScene(
				firstBandOfScene,
				analysisInfo);
	}

	public static void writeScene(
			final SimpleFeatureType sceneType,
			final SimpleFeature firstBandOfScene,
			final IndexWriter sceneWriter ) {
		final SimpleFeatureBuilder bldr = new SimpleFeatureBuilder(
				sceneType);
		String fid = null;
		for (int i = 0; i < sceneType.getAttributeCount(); i++) {
			final AttributeDescriptor attr = sceneType.getDescriptor(i);
			final String attrName = attr.getLocalName();
			final Object attrValue = firstBandOfScene.getAttribute(attrName);
			if (attrValue != null) {
				bldr.set(
						i,
						attrValue);
				if (attrName.equals(SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME)) {
					fid = attrValue.toString();
				}
			}
		}
		if (fid != null) {
			try {
				sceneWriter.write(bldr.buildFeature(fid));
			}
			catch (IOException e) {
				LOGGER.error(
						"Unable to write scene",
						e);
			}
		}
	}
}