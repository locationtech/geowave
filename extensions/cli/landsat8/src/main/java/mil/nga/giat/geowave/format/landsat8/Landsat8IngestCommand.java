package mil.nga.giat.geowave.format.landsat8;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ui.freemarker.FreeMarkerTemplateUtils;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import freemarker.template.Template;
import freemarker.template.TemplateException;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "ingest", parentOperation = Landsat8Section.class)
@Parameters(commandDescription = "Ingest routine for locally downloading Landsat 8 imagery and ingesting it into GeoWave")
public class Landsat8IngestCommand extends
		Landsat8DownloadCommand
{
	private final static Logger LOGGER = LoggerFactory.getLogger(Landsat8IngestCommand.class);
	private static final int LONGITUDE_BITS = 27;
	private static final int LATITUDE_BITS = 27;
	private static final int TIME_BITS = 8;

	@Parameter(description = "<storename> <comma delimited index/group list>")
	private final List<String> parameters = new ArrayList<String>();
	protected Landsat8IngestCommandLineOptions ingestOptions;
	List<SimpleFeature> lastSceneBands = new ArrayList<SimpleFeature>();
	private Template coverageNameTemplate;
	private Map<String, IndexWriter> writerCache;
	private final Map<String, RasterDataAdapter> adapterCache = new HashMap<String, RasterDataAdapter>();

	private DataStorePluginOptions storeOptions = null;
	private DataStore store = null;
	PrimaryIndex[] indices = null;
	private List<IndexPluginOptions> indexOptions = null;

	public Landsat8IngestCommand() {}

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
			if (storeOptions == null) {
				final StoreLoader inputStoreLoader = new StoreLoader(
						inputStoreName);
				if (!inputStoreLoader.loadFromConfig(configFile)) {
					throw new ParameterException(
							"Cannot find store name: " + inputStoreLoader.getStoreName());
				}
				storeOptions = inputStoreLoader.getDataStorePlugin();
				store = storeOptions.createDataStore();
			}
			// Load the Indexes
			if (indexOptions == null) {
				final IndexLoader indexLoader = new IndexLoader(
						indexList);
				if (!indexLoader.loadFromConfig(configFile)) {
					throw new ParameterException(
							"Cannot find index(s) by name: " + indexList);
				}
				indexOptions = indexLoader.getLoadedIndexes();
			}

			indices = new PrimaryIndex[indexOptions.size()];
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
			// final TimeDefinition timeDefinition = new TimeDefinition(
			// new Landsat8TemporalBinningStrategy());
			// final Index index = accumuloOptions.getIndex(
			// new Index[] {
			// IndexType.SPATIAL_RASTER.createDefaultIndex(),
			// new CustomIdIndex(
			// TieredSFCIndexFactory.createEqualIntervalPrecisionTieredStrategy(
			// new NumericDimensionDefinition[] {
			// new LatitudeDefinition(),
			// new LongitudeDefinition(),
			// timeDefinition
			// },
			// new int[] {
			// LONGITUDE_BITS,
			// LATITUDE_BITS,
			// TIME_BITS
			// },
			// SFCType.HILBERT),
			// new BasicIndexModel(
			// new DimensionField[] {
			// new LongitudeField(),
			// new LatitudeField(),
			// new TimeField(
			// timeDefinition)
			// }),
			// new ByteArrayId(
			// "SpatialTemporalLandsat8Index"))
			// });
			// writer = geowaveDataStore.createIndexWriter(
			// index);

			super.runInternal(params);
		}
		finally {
			for (final IndexWriter writer : writerCache.values()) {
				if (writer != null) {
					try {
						writer.close();
					}
					catch (final IOException e) {
						LOGGER.error(
								"Unable to close Accumulo writer",
								e);
					}
				}
			}
		}
	}

	@Override
	protected void nextBand(
			final SimpleFeature band,
			final AnalysisInfo analysisInfo ) {
		super.nextBand(
				band,
				analysisInfo);
		// if (ingestOptions.isCoveragePerBand()) {
		// ingest this band
		// convert the simplefeature into a map to resolve the coverage name
		// using a user supplied freemarker template
		final Map<String, Object> model = new HashMap<String, Object>();
		final SimpleFeatureType type = band.getFeatureType();
		for (final AttributeDescriptor attr : type.getAttributeDescriptors()) {
			final String attrName = attr.getLocalName();
			final Object attrValue = band.getAttribute(attrName);
			if (attrValue != null) {
				model.put(
						attrName,
						attrValue);
			}
		}
		try {
			final String coverageName = FreeMarkerTemplateUtils.processTemplateIntoString(
					coverageNameTemplate,
					model);
			final File geotiffFile = Landsat8DownloadCommand.getDownloadFile(
					band,
					landsatOptions.getWorkspaceDir());
			final GeoTiffReader reader = new GeoTiffReader(
					geotiffFile);
			final GridCoverage2D coverage = reader.read(null);
			RasterDataAdapter adapter = adapterCache.get(coverageName);
			IndexWriter writer = writerCache.get(coverageName);
			// TODO use converter nextCov = Landsat8BandConverterSpi
			final GridCoverage2D nextCov = coverage;
			if (adapter == null) {
				final Map<String, String> metadata = new HashMap<String, String>();
				final String[] mdNames = reader.getMetadataNames(coverage.getName().toString());
				if ((mdNames != null) && (mdNames.length > 0)) {
					for (final String mdName : mdNames) {
						metadata.put(
								mdName,
								reader.getMetadataValue(
										coverageName,
										mdName));
					}
				}
				adapter = new RasterDataAdapter(
						coverageName,
						metadata,
						nextCov,
						ingestOptions.getTileSize(),
						ingestOptions.isCreatePyramid(),
						ingestOptions.isCreateHistogram(),
						new double[][] {
							new double[] {
								0
							}
						},
						new NoDataMergeStrategy());
				adapterCache.put(
						coverageName,
						adapter);
				writer = store.createWriter(
						adapter,
						indices);
				writerCache.put(
						coverageName,
						writer);
			}
			writer.write(nextCov);
		}
		catch (IOException | TemplateException e) {
			LOGGER.error(
					"Unable to ingest band " + band.getID() + " because coverage name cannot be resolved from template",
					e);
		}
		// }
		// else {
		// lastSceneBands.add(band);
		// }
	}

	@Override
	protected void lastSceneComplete(
			final AnalysisInfo analysisInfo ) {
		super.lastSceneComplete(analysisInfo);
		processLastScene();
	}

	@Override
	protected void nextScene(
			final SimpleFeature firstBandOfScene,
			final AnalysisInfo analysisInfo ) {
		processLastScene();
		super.nextScene(
				firstBandOfScene,
				analysisInfo);
	}

	protected void processLastScene() {
		// if (!ingestOptions.isCoveragePerBand()) {
		// if (!lastSceneBands.isEmpty()) {
		// TODO ingest as single image for all bands
		// do we need to capture all possible permutations of bands (ie.
		// this scene may not contain all bands for this coverage)?

		// also we need to consider supersampling if the panchromatic
		// band (B8) is provided, because it will be twice the
		// resolution of the other bands

		// }
		// lastSceneBands.clear();
		// }
	}

}
