package mil.nga.giat.geowave.service.grpc;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.store.ContentFeatureCollection;
import org.geotools.factory.FactoryRegistryException;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;

import io.grpcshaded.stub.StreamObserver;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWavePluginConfig;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWavePluginException;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialTemporalQuery;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalRange;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.cli.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.cli.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.service.grpc.protobuf.CQLQueryParameters;
import mil.nga.giat.geowave.service.grpc.protobuf.Feature;
import mil.nga.giat.geowave.service.grpc.protobuf.FeatureAttribute;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse;
import mil.nga.giat.geowave.service.grpc.protobuf.SpatialQueryParameters;
import mil.nga.giat.geowave.service.grpc.protobuf.SpatialTemporalQueryParameters;
import mil.nga.giat.geowave.service.grpc.protobuf.TemporalConstraints;
import mil.nga.giat.geowave.service.grpc.protobuf.VectorIngestParameters;
import mil.nga.giat.geowave.service.grpc.protobuf.VectorGrpc;
import mil.nga.giat.geowave.service.grpc.protobuf.VectorQueryParameters;

public class GeoWaveGrpcVectorService extends
		VectorGrpc.VectorImplBase
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcVectorService.class.getName());

	@Override
	public void vectorQuery(
			VectorQueryParameters request,
			StreamObserver<Feature> responseObserver ) {
		final String storeName = request.getStoreName();
		final StoreLoader storeLoader = new StoreLoader(
				storeName);
		// first check to make sure the data store exists
		if (!storeLoader.loadFromConfig(GeoWaveGrpcServiceOptions.geowaveConfigFile)) {
			throw new ParameterException(
					"Cannot find store name: " + storeLoader.getStoreName());
		}

		GeoWaveGTDataStore gtStore = null;
		try {
			gtStore = new GeoWaveGTDataStore(
					new GeoWavePluginConfig(
							storeLoader.getDataStorePlugin()));
		}
		catch (final IOException | GeoWavePluginException e) {
			LOGGER.error(
					"Exception encountered instantiating GeoWaveGTDataStore",
					e);
		}

		Filter filter = null;
		try {
			filter = CQL.toFilter(request.getQuery());
		}
		catch (final CQLException e) {
			LOGGER.error(
					"Exception encountered creating filter from CQL",
					e);
		}

		ContentFeatureCollection featureCollection = null;
		try {
			ByteArrayId adapterId = new ByteArrayId(
					request.getAdapterId().toByteArray());
			featureCollection = gtStore.getFeatureSource(
					adapterId.getString()).getFeatures(
					filter);
		}
		catch (final IOException | NullPointerException e) {
			LOGGER.error(
					"Exception encountered getting feature collection",
					e);
		}

		try (final SimpleFeatureIterator iterator = featureCollection.features()) {

			while (iterator.hasNext()) {
				final SimpleFeature simpleFeature = iterator.next();
				final SimpleFeatureType type = simpleFeature.getType();
				final Feature.Builder b = Feature.newBuilder();
				final FeatureAttribute.Builder attBuilder = FeatureAttribute.newBuilder();

				for (int i = 0; i < type.getAttributeDescriptors().size(); i++) {
					SetAttributeBuilderValue(
							simpleFeature.getAttribute(i),
							attBuilder);
					b.putAttributes(
							type.getAttributeDescriptors().get(
									i).getLocalName(),
							attBuilder.build());
					/*
					 * b.putAttributes( type.getAttributeDescriptors().get(
					 * i).getLocalName(), simpleFeature.getAttribute(i) == null
					 * ? "" : simpleFeature.getAttribute( i).toString());
					 */
				}
				final Feature f = b.build();
				responseObserver.onNext(f);
			}
			responseObserver.onCompleted();
		}
		catch (final NullPointerException e) {
			LOGGER.error(
					"Exception encountered",
					e);
		}
	}

	@Override
	public StreamObserver<VectorIngestParameters> vectorIngest(
			StreamObserver<StringResponse> responseObserver ) {
		return new StreamObserver<VectorIngestParameters>() {
			private boolean firstFeature = true;
			private String storeName = null;
			private DataStore dataStore = null;
			private ByteArrayId adapterId = null;
			private ByteArrayId indexId = null;
			private IndexWriter<SimpleFeature> writer = null;

			private GeotoolsFeatureDataAdapter adapter = null;
			private PrimaryIndex pIndex = null;
			private SimpleFeatureTypeBuilder typeBuilder = null;
			private SimpleFeatureBuilder featureBuilder = null;

			private static final int batchSize = 100;
			private int batchCount = 0;
			private int totalCount = 0;

			@Override
			public void onNext(
					VectorIngestParameters f ) {
				if (firstFeature) {
					firstFeature = false;

					// parse top level required parameters
					storeName = f.getBaseParams().getStoreName();
					final StoreLoader storeLoader = new StoreLoader(
							storeName);

					adapterId = new ByteArrayId(
							f.getBaseParams().getAdapterId().toByteArray());
					indexId = new ByteArrayId(
							f.getBaseParams().getIndexId().toByteArray());

					// In order to store data we need to determine the type of
					// the feature data
					// This only needs to happen once
					if (typeBuilder == null) {
						typeBuilder = new SimpleFeatureTypeBuilder();

						for (Map.Entry<String, FeatureAttribute> mapEntry : f.getFeatureMap().entrySet()) {
							switch (mapEntry.getValue().getValueCase()) {
								case VALSTRING: {
									typeBuilder.add(
											mapEntry.getKey(),
											String.class);
									break;
								}
								case VALINT32: {
									typeBuilder.add(
											mapEntry.getKey(),
											Integer.class);
									break;
								}
								case VALINT64: {
									typeBuilder.add(
											mapEntry.getKey(),
											Long.class);
									break;
								}
								case VALFLOAT: {
									typeBuilder.add(
											mapEntry.getKey(),
											Float.class);
									break;
								}
								case VALDOUBLE: {
									typeBuilder.add(
											mapEntry.getKey(),
											Double.class);
									break;
								}
								case VALGEOMETRY: {
									typeBuilder.add(
											mapEntry.getKey(),
											Geometry.class);
									break;
								}
								default:
									break;
							}
							;
						}
					}
					// This a factory class that builds simple feature objects
					// based
					// on the
					// type
					typeBuilder.setName(adapterId.getString());
					SimpleFeatureType featureType = typeBuilder.buildFeatureType();
					featureBuilder = new SimpleFeatureBuilder(
							featureType);

					// get a handle to the relevant stores
					if (!storeLoader.loadFromConfig(GeoWaveGrpcServiceOptions.geowaveConfigFile)) {
						throw new ParameterException(
								"Cannot find store name: " + storeLoader.getStoreName());
					}

					dataStore = storeLoader.createDataStore();
					final AdapterStore adapterStore = storeLoader.createAdapterStore();
					final IndexStore indexStore = storeLoader.createIndexStore();

					adapter = (GeotoolsFeatureDataAdapter) adapterStore.getAdapter(adapterId);
					if (adapter == null) {
						adapter = new FeatureDataAdapter(
								featureType);
					}

					// Load the Indexes
					final IndexLoader indexLoader = new IndexLoader(
							indexId.getString());
					if (!indexLoader.loadFromConfig(GeoWaveGrpcServiceOptions.geowaveConfigFile)) {
						throw new ParameterException(
								"Cannot find index(s) by name: " + indexId.toString());
					}
					final List<IndexPluginOptions> indexOptions = indexLoader.getLoadedIndexes();

					// assuming one index for now
					pIndex = indexOptions.get(
							0).createPrimaryIndex();// (PrimaryIndex)
													// indexStore.getIndex(indexId);
					if (pIndex == null) {
						throw new ParameterException(
								"Failed to instantiate primary index");
					}

					// create index writer to actually write data
					try {
						writer = dataStore.createWriter(
								adapter,
								pIndex);
					}
					catch (final IOException e) {
						LOGGER.error(
								"Unable to create index writer",
								e);
					}

				}// end first-time initialization

				// Set the values for all the attributes in the feature
				for (Map.Entry<String, FeatureAttribute> attribute : f.getFeatureMap().entrySet()) {
					switch (attribute.getValue().getValueCase()) {
						case VALSTRING: {
							featureBuilder.set(
									attribute.getKey(),
									attribute.getValue().getValString());
							break;
						}
						case VALINT32: {
							featureBuilder.set(
									attribute.getKey(),
									attribute.getValue().getValInt32());
							break;
						}
						case VALINT64: {
							featureBuilder.set(
									attribute.getKey(),
									attribute.getValue().getValInt64());
							break;
						}
						case VALFLOAT: {
							featureBuilder.set(
									attribute.getKey(),
									attribute.getValue().getValFloat());
							break;
						}
						case VALDOUBLE: {
							featureBuilder.set(
									attribute.getKey(),
									attribute.getValue().getValDouble());
							break;
						}
						case VALGEOMETRY: {

							Geometry geom = null;
							try {
								geom = new WKTReader(
										JTSFactoryFinder.getGeometryFactory()).read(attribute
										.getValue()
										.getValGeometry());
							}
							catch (FactoryRegistryException | com.vividsolutions.jts.io.ParseException e) {
								LOGGER.error(
										"Failed to parse string for geometry",
										e);
							}

							if (geom != null) {
								featureBuilder.set(
										attribute.getKey(),
										geom);
							}
							break;
						}
						default:
							break;
					}
					;
				}
				final SimpleFeature sf = featureBuilder.buildFeature(String.valueOf(totalCount));
				InsertionIds ids = writer.write(sf);

				// The writer is finally flushed and closed in the methods for
				// onComplete and onError
				if (++batchCount >= batchSize) {
					// writer.flush();
					batchCount = 0;
				}

				StringResponse resp = StringResponse.newBuilder().setResponseValue(
						String.valueOf(++totalCount)).build();
				responseObserver.onNext(resp);
			}

			@Override
			public void onError(
					Throwable t ) {
				LOGGER.error(
						"Exception encountered during vectorIngest",
						t);
				try {
					writer.flush();
					writer.close();
				}
				catch (IOException e) {
					LOGGER.error(
							"Unable to close index writer",
							e);
				}

				StringResponse resp = StringResponse.newBuilder().setResponseValue(
						"Error during ingest: ").build();
				responseObserver.onNext(resp);
				responseObserver.onCompleted();
			}

			@Override
			public void onCompleted() {
				try {
					writer.flush();
					writer.close();
				}
				catch (IOException e) {
					LOGGER.error(
							"Unable to close index writer",
							e);
				}
				StringResponse resp = StringResponse.newBuilder().setResponseValue(
						"Ingest completed successfully").build();
				responseObserver.onNext(resp);
				responseObserver.onCompleted();
			}
		};
	}

	@Override
	public void cqlQuery(
			CQLQueryParameters request,
			StreamObserver<Feature> responseObserver ) {

		final String cql = request.getCql();
		final String storeName = request.getBaseParams().getStoreName();
		final StoreLoader storeLoader = new StoreLoader(
				storeName);

		ByteArrayId adapterId = new ByteArrayId(
				request.getBaseParams().getAdapterId().toByteArray());
		ByteArrayId indexId = new ByteArrayId(
				request.getBaseParams().getIndexId().toByteArray());

		if (adapterId.getString().equalsIgnoreCase(
				"")) adapterId = null;
		if (indexId.getString().equalsIgnoreCase(
				"")) indexId = null;

		// first check to make sure the data store exists
		if (!storeLoader.loadFromConfig(GeoWaveGrpcServiceOptions.geowaveConfigFile)) {
			throw new ParameterException(
					"Cannot find store name: " + storeLoader.getStoreName());
		}

		// get a handle to the relevant stores
		final DataStore dataStore = storeLoader.createDataStore();
		final AdapterStore adapterStore = storeLoader.createAdapterStore();
		final IndexStore indexStore = storeLoader.createIndexStore();

		GeotoolsFeatureDataAdapter adapter = null;
		PrimaryIndex pIndex = null;

		if (adapterId != null) {
			adapter = (GeotoolsFeatureDataAdapter) adapterStore.getAdapter(adapterId);
		}

		if (indexId != null) {
			pIndex = (PrimaryIndex) indexStore.getIndex(indexId);
		}

		try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
				new QueryOptions(
						adapterId,
						indexId),
				CQLQuery.createOptimalQuery(
						cql,
						adapter,
						pIndex))) {

			while (iterator.hasNext()) {
				final SimpleFeature simpleFeature = iterator.next();
				final SimpleFeatureType type = simpleFeature.getType();
				final Feature.Builder b = Feature.newBuilder();
				final FeatureAttribute.Builder attBuilder = FeatureAttribute.newBuilder();

				for (int i = 0; i < type.getAttributeDescriptors().size(); i++) {
					SetAttributeBuilderValue(
							simpleFeature.getAttribute(i),
							attBuilder);
					b.putAttributes(
							type.getAttributeDescriptors().get(
									i).getLocalName(),
							attBuilder.build());
				}
				final Feature f = b.build();
				responseObserver.onNext(f);
			}
			responseObserver.onCompleted();
		}
		catch (final CQLException e) {
			LOGGER.error(
					"Exception encountered CQL.createOptimalQuery",
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Exception encountered closing iterator",
					e);
		}

	}

	public void spatialQuery(
			SpatialQueryParameters request,
			StreamObserver<Feature> responseObserver ) {

		final String storeName = request.getBaseParams().getStoreName();
		final StoreLoader storeLoader = new StoreLoader(
				storeName);

		ByteArrayId adapterId = new ByteArrayId(
				request.getBaseParams().getAdapterId().toByteArray());
		ByteArrayId indexId = new ByteArrayId(
				request.getBaseParams().getIndexId().toByteArray());

		if (adapterId.getString().equalsIgnoreCase(
				"")) adapterId = null;
		if (indexId.getString().equalsIgnoreCase(
				"")) indexId = null;

		// first check to make sure the data store exists
		if (!storeLoader.loadFromConfig(GeoWaveGrpcServiceOptions.geowaveConfigFile)) {
			throw new ParameterException(
					"Cannot find store name: " + storeLoader.getStoreName());
		}

		final DataStore dataStore = storeLoader.createDataStore();

		final String geomDefinition = request.getGeometry();
		Geometry queryGeom = null;

		try {
			queryGeom = new WKTReader(
					JTSFactoryFinder.getGeometryFactory()).read(geomDefinition);
		}
		catch (final FactoryRegistryException | com.vividsolutions.jts.io.ParseException e) {
			LOGGER.error(
					"Exception encountered creating query geometry",
					e);
		}

		final QueryOptions options = new QueryOptions(
				adapterId,
				indexId);

		try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
				options,
				new SpatialQuery(
						queryGeom))) {
			while (iterator.hasNext()) {
				final SimpleFeature simpleFeature = iterator.next();
				final SimpleFeatureType type = simpleFeature.getType();
				final Feature.Builder b = Feature.newBuilder();
				final FeatureAttribute.Builder attBuilder = FeatureAttribute.newBuilder();

				for (int i = 0; i < type.getAttributeDescriptors().size(); i++) {
					SetAttributeBuilderValue(
							simpleFeature.getAttribute(i),
							attBuilder);
					b.putAttributes(
							type.getAttributeDescriptors().get(
									i).getLocalName(),
							attBuilder.build());
				}
				final Feature f = b.build();
				responseObserver.onNext(f);
			}
			responseObserver.onCompleted();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Exception encountered closing iterator",
					e);
		}
	}

	@Override
	public void spatialTemporalQuery(
			SpatialTemporalQueryParameters request,
			StreamObserver<Feature> responseObserver ) {

		final String storeName = request.getSpatialParams().getBaseParams().getStoreName();
		final StoreLoader storeLoader = new StoreLoader(
				storeName);

		// first check to make sure the data store exists
		if (!storeLoader.loadFromConfig(GeoWaveGrpcServiceOptions.geowaveConfigFile)) {
			throw new ParameterException(
					"Cannot find store name: " + storeLoader.getStoreName());
		}

		final DataStore dataStore = storeLoader.createDataStore();

		ByteArrayId adapterId = new ByteArrayId(
				request.getSpatialParams().getBaseParams().getAdapterId().toByteArray());
		ByteArrayId indexId = new ByteArrayId(
				request.getSpatialParams().getBaseParams().getIndexId().toByteArray());

		if (adapterId.getString().equalsIgnoreCase(
				"")) adapterId = null;
		if (indexId.getString().equalsIgnoreCase(
				"")) indexId = null;

		final int constraintCount = request.getTemporalConstraintsCount();
		final ArrayList<TemporalRange> temporalRanges = new ArrayList<TemporalRange>();
		for (int i = 0; i < constraintCount; i++) {
			final TemporalConstraints t = request.getTemporalConstraints(i);

			try {
				temporalRanges.add(new TemporalRange(
						DateUtilities.parseISO(t.getStartTime()),
						DateUtilities.parseISO(t.getEndTime())));
			}
			catch (final ParseException e) {
				LOGGER.error(
						"Exception encountered parsing date",
						e);
			}
		}

		final String geomDefinition = request.getSpatialParams().getGeometry();
		Geometry queryGeom = null;
		mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraints temporalConstraints = null;

		try {
			queryGeom = new WKTReader(
					JTSFactoryFinder.getGeometryFactory()).read(geomDefinition);
		}
		catch (final FactoryRegistryException | com.vividsolutions.jts.io.ParseException e) {
			LOGGER.error(
					"Exception encountered creating query geometry",
					e);
		}

		temporalConstraints = new mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraints(
				temporalRanges,
				"ignored"); // the name is not used in this case

		final QueryOptions options = new QueryOptions(
				adapterId,
				indexId);
		final CompareOperation op = CompareOperation.valueOf(request.getCompareOperation());
		final SpatialTemporalQuery spatialTemporalQuery = new SpatialTemporalQuery(
				temporalConstraints,
				queryGeom,
				op);

		try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
				options,
				spatialTemporalQuery)) {
			while (iterator.hasNext()) {
				SimpleFeature simpleFeature = iterator.next();
				SimpleFeatureType type = simpleFeature.getType();
				Feature.Builder b = Feature.newBuilder();
				final FeatureAttribute.Builder attBuilder = FeatureAttribute.newBuilder();

				for (int i = 0; i < type.getAttributeDescriptors().size(); i++) {
					SetAttributeBuilderValue(
							simpleFeature.getAttribute(i),
							attBuilder);
					b.putAttributes(
							type.getAttributeDescriptors().get(
									i).getLocalName(),
							attBuilder.build());
				}
				Feature f = b.build();
				responseObserver.onNext(f);
			}
			responseObserver.onCompleted();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Exception encountered closing iterator",
					e);
		}
	}

	private void SetAttributeBuilderValue(
			final Object simpleFeatureAttribute,
			final FeatureAttribute.Builder attBuilder ) {
		switch (simpleFeatureAttribute.getClass().getSimpleName()) {
			case "String": {
				attBuilder.setValString((String) simpleFeatureAttribute);
				break;
			}
			case "Integer": {
				attBuilder.setValInt32((Integer) simpleFeatureAttribute);
				break;
			}
			case "Long": {
				attBuilder.setValInt64((Long) simpleFeatureAttribute);
				break;
			}
			case "Float": {
				attBuilder.setValFloat((Float) simpleFeatureAttribute);
				break;
			}
			case "Double": {
				attBuilder.setValDouble((Double) simpleFeatureAttribute);
				break;
			}
			case "Geoemetry": {
				break;
			}
			default:
				break;
		}
		;
	}
}
