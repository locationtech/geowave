package mil.nga.giat.geowave.analytic.clustering;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.filter.FilterFactoryImpl;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryType;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.AnalyticFeature.ClusterFeatureAttribute;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.clustering.exception.MatchingCentroidNotFoundException;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.StoreParameters;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;

/**
 * 
 * Manages the population of centroids by group id and batch id.
 * 
 * Properties:
 * 
 * @formatter:off
 * 
 *                "CentroidManagerGeoWave.Centroid.WrapperFactoryClass" -
 *                {@link AnalyticItemWrapperFactory} to extract wrap spatial
 *                objects with Centroid management function
 * 
 *                "CentroidManagerGeoWave.Centroid.DataTypeId" -> The data type
 *                ID of the centroid simple feature
 * 
 *                "CentroidManagerGeoWave.Centroid.IndexId" -> The GeoWave index
 *                ID of the centroid simple feature
 * 
 *                "CentroidManagerGeoWave.Global.BatchId" -> Batch ID for
 *                updates
 * 
 *                "CentroidManagerGeoWave.Global.Zookeeper" -> Zookeeper URL
 * 
 *                "CentroidManagerGeoWave.Global.AccumuloInstance" -> Accumulo
 *                Instance Name
 * 
 *                "CentroidManagerGeoWave.Global.AccumuloUser" -> Accumulo User
 *                name
 * 
 *                "CentroidManagerGeoWave.Global.AccumuloPassword" -> Accumulo
 *                Password
 * 
 *                "CentroidManagerGeoWave.Global.AccumuloNamespace" -> Accumulo
 *                Table Namespace
 * 
 *                "CentroidManagerGeoWave.Common.AccumuloConnectFactory" ->
 *                {@link BasicAccumuloOperationsFactory}
 * 
 * @formatter:on
 * 
 * @param <T>
 *            The item type used to represent a centroid.
 */
public class CentroidManagerGeoWave<T> implements
		CentroidManager<T>
{
	final static Logger LOGGER = LoggerFactory.getLogger(CentroidManagerGeoWave.class);
	private static final ParameterEnum<?>[] MY_PARAMS = new ParameterEnum[] {
		StoreParameters.StoreParam.INPUT_STORE,
		GlobalParameters.Global.BATCH_ID,
		CentroidParameters.Centroid.DATA_TYPE_ID,
		CentroidParameters.Centroid.DATA_NAMESPACE_URI,
		CentroidParameters.Centroid.INDEX_ID,
		CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
		CentroidParameters.Centroid.ZOOM_LEVEL
	};
	private String centroidDataTypeId;
	private String batchId;
	private int level = 0;

	private AnalyticItemWrapperFactory<T> centroidFactory;
	@SuppressWarnings("rawtypes")
	private GeotoolsFeatureDataAdapter adapter;

	private DataStore dataStore;
	private IndexStore indexStore;
	private AdapterStore adapterStore;
	private PrimaryIndex index;

	public CentroidManagerGeoWave(
			final DataStore dataStore,
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final AnalyticItemWrapperFactory<T> centroidFactory,
			final String centroidDataTypeId,
			final String indexId,
			final String batchId,
			final int level ) {
		this.centroidFactory = centroidFactory;
		this.centroidDataTypeId = centroidDataTypeId;
		this.level = level;
		this.batchId = batchId;
		this.dataStore = dataStore;
		this.indexStore = indexStore;
		index = (PrimaryIndex) indexStore.getIndex(new ByteArrayId(
				StringUtils.stringToBinary(indexId)));
		this.adapterStore = adapterStore;
		adapter = (GeotoolsFeatureDataAdapter) adapterStore.getAdapter(new ByteArrayId(
				StringUtils.stringToBinary(centroidDataTypeId)));
	}

	@SuppressWarnings("unchecked")
	public CentroidManagerGeoWave(
			final PropertyManagement properties )
			throws IOException {
		final Class<?> scope = CentroidManagerGeoWave.class;
		final Configuration configuration = new Configuration();
		properties.setJobConfiguration(
				configuration,
				scope);
		init(
				Job.getInstance(configuration),
				scope,
				LOGGER);
	}

	public CentroidManagerGeoWave(
			final JobContext context,
			final Class<?> scope )
			throws IOException {
		this(
				context,
				scope,
				LOGGER);
	}

	public CentroidManagerGeoWave(
			final JobContext context,
			final Class<?> scope,
			final Logger logger )
			throws IOException {
		init(
				context,
				scope,
				logger);
	}

	private void init(
			final JobContext context,
			final Class<?> scope,
			final Logger logger )
			throws IOException {
		final ScopedJobConfiguration scopedJob = new ScopedJobConfiguration(
				context.getConfiguration(),
				scope,
				logger);
		try {
			centroidFactory = (AnalyticItemWrapperFactory<T>) CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS
					.getHelper()
					.getValue(
							context,
							scope,
							CentroidItemWrapperFactory.class);
			centroidFactory.initialize(
					context,
					scope,
					logger);

		}
		catch (final Exception e1) {
			LOGGER.error("Cannot instantiate " + GeoWaveConfiguratorBase.enumToConfKey(
					this.getClass(),
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS));
			throw new IOException(
					e1);
		}

		this.level = scopedJob.getInt(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				1);

		centroidDataTypeId = scopedJob.getString(
				CentroidParameters.Centroid.DATA_TYPE_ID,
				"centroid");

		batchId = scopedJob.getString(
				GlobalParameters.Global.BATCH_ID,
				Long.toString(Calendar.getInstance().getTime().getTime()));

		final String indexId = scopedJob.getString(
				CentroidParameters.Centroid.INDEX_ID,
				new SpatialDimensionalityTypeProvider().createPrimaryIndex().getId().getString());
		PersistableStore store = (PersistableStore) StoreParameters.StoreParam.INPUT_STORE.getHelper().getValue(
				context,
				scope,
				null);

		dataStore = store.getDataStoreOptions().createDataStore();
		indexStore = store.getDataStoreOptions().createIndexStore();
		index = (PrimaryIndex) indexStore.getIndex(new ByteArrayId(
				StringUtils.stringToBinary(indexId)));
		adapterStore = store.getDataStoreOptions().createAdapterStore();
		adapter = (GeotoolsFeatureDataAdapter) adapterStore.getAdapter(new ByteArrayId(
				StringUtils.stringToBinary(centroidDataTypeId)));
	}

	/**
	 * Creates a new centroid based on the old centroid with new coordinates and
	 * dimension values
	 * 
	 * @param feature
	 * @param coordinate
	 * @param extraNames
	 * @param extraValues
	 * @return
	 */
	@Override
	public AnalyticItemWrapper<T> createNextCentroid(
			final T feature,
			final String groupID,
			final Coordinate coordinate,
			final String[] extraNames,
			final double[] extraValues ) {
		return centroidFactory.createNextItem(
				feature,
				groupID,
				coordinate,
				extraNames,
				extraValues);
	}

	private final int capacity = 100;
	private final LRUMap groupToCentroid = new LRUMap(
			capacity);

	@Override
	public void clear() {
		groupToCentroid.clear();
	}

	@Override
	public void delete(
			final String[] dataIds )
			throws IOException {
		final ByteArrayId adapterId = new ByteArrayId(
				StringUtils.stringToBinary(centroidDataTypeId));
		for (final String dataId : dataIds) {

			if (dataId != null) {
				dataStore.delete(
						new QueryOptions(
								adapterId,
								index.getId()),
						new DataIdQuery(
								adapterId,
								new ByteArrayId(
										StringUtils.stringToBinary(dataId))));
			}
		}
	}

	@Override
	public List<String> getAllCentroidGroups()
			throws IOException {
		final List<String> groups = new ArrayList<String>();
		final CloseableIterator<T> it = getRawCentroids(
				this.batchId,
				null);
		while (it.hasNext()) {
			final AnalyticItemWrapper<T> item = centroidFactory.create(it.next());
			final String groupID = item.getGroupID();
			int pos = groups.indexOf(groupID);
			if (pos < 0) {
				pos = groups.size();
				groups.add(groupID);
			}
			// cache the first set
			if (pos < capacity) {
				getCentroidsForGroup(groupID);
			}
		}
		it.close();
		return groups;
	}

	@Override
	public List<AnalyticItemWrapper<T>> getCentroidsForGroup(
			final String groupID )
			throws IOException {
		return getCentroidsForGroup(
				this.batchId,
				groupID);
	}

	@Override
	public List<AnalyticItemWrapper<T>> getCentroidsForGroup(
			final String batchID,
			final String groupID )
			throws IOException {
		final String lookupGroup = (groupID == null) ? "##" : groupID;

		final Pair<String, String> gid = Pair.of(
				batchID,
				lookupGroup);
		@SuppressWarnings("unchecked")
		List<AnalyticItemWrapper<T>> centroids = (List<AnalyticItemWrapper<T>>) groupToCentroid.get(gid);
		if (centroids == null) {
			centroids = groupID == null ? loadCentroids(
					batchID,
					null) : loadCentroids(
					batchID,
					groupID);
			groupToCentroid.put(
					gid,
					centroids);
		}
		return centroids;
	}

	@Override
	public AnalyticItemWrapper<T> getCentroidById(
			final String id,
			final String groupID )
			throws IOException,
			MatchingCentroidNotFoundException {
		for (final AnalyticItemWrapper<T> centroid : this.getCentroidsForGroup(groupID)) {
			if (centroid.getID().equals(
					id)) {
				return centroid;
			}
		}
		throw new MatchingCentroidNotFoundException(
				id);
	}

	private List<AnalyticItemWrapper<T>> loadCentroids(
			final String batchID,
			final String groupID )
			throws IOException {
		final List<AnalyticItemWrapper<T>> centroids = new ArrayList<AnalyticItemWrapper<T>>();
		try {

			CloseableIterator<T> it = null;

			try {
				it = this.getRawCentroids(
						batchID,
						groupID);
				while (it.hasNext()) {
					centroids.add(centroidFactory.create(it.next()));
				}
				return centroids;
			}
			finally {
				if (it != null) {
					try {
						it.close();
					}
					catch (final IOException e) {
						LOGGER.warn(
								"Unable to close the iterator",
								e);
					}
				}
			}

		}
		catch (final IOException e) {
			LOGGER.error("Cannot load centroids");
			throw new IOException(
					e);
		}

	}

	@Override
	@SuppressWarnings("unchecked")
	public AnalyticItemWrapper<T> getCentroid(
			final String dataId ) {
		final ByteArrayId adapterId = new ByteArrayId(
				StringUtils.stringToBinary(centroidDataTypeId));
		try (CloseableIterator<T> it = dataStore.query(
				new QueryOptions(
						adapterId,
						index.getId()),
				new DataIdQuery(
						adapterId,
						new ByteArrayId(
								StringUtils.stringToBinary(dataId))))) {
			if (it.hasNext()) {
				return centroidFactory.create(it.next());
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Failed to fined centroid " + dataId.toString(),
					e);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	protected CloseableIterator<T> getRawCentroids(
			final String batchId,
			final String groupID )
			throws IOException {

		final FilterFactoryImpl factory = new FilterFactoryImpl();
		final Expression expB1 = factory.property(ClusterFeatureAttribute.BATCH_ID.attrName());
		final Expression expB2 = factory.literal(batchId);

		final Filter batchIdFilter = factory.equal(
				expB1,
				expB2,
				false);

		Filter finalFilter = batchIdFilter;
		if (groupID != null) {
			final Expression exp1 = factory.property(ClusterFeatureAttribute.GROUP_ID.attrName());
			final Expression exp2 = factory.literal(groupID);
			// ignore levels for group IDS
			finalFilter = factory.and(
					factory.equal(
							exp1,
							exp2,
							false),
					batchIdFilter);
		}
		else if (level > 0) {
			final Expression exp1 = factory.property(ClusterFeatureAttribute.ZOOM_LEVEL.attrName());
			final Expression exp2 = factory.literal(level);
			finalFilter = factory.and(
					factory.equal(
							exp1,
							exp2,
							false),
					batchIdFilter);
		}
		return (CloseableIterator<T>) dataStore.query(
				new QueryOptions(
						adapter,
						index),
				new CQLQuery(
						null,
						finalFilter,
						adapter));
	}

	@SuppressWarnings("unchecked")
	public void transferBatch(
			final String fromBatchId,
			final String groupID )
			throws IOException {
		final CloseableIterator<T> it = getRawCentroids(
				fromBatchId,
				groupID);
		int count = 0;
		try (final IndexWriter indexWriter = dataStore.createWriter(
				adapter,
				index)) {
			while (it.hasNext()) {
				final AnalyticItemWrapper<T> item = centroidFactory.create(it.next());
				item.setBatchID(this.batchId);
				count++;

				indexWriter.write(item.getWrappedItem());
			}
			it.close();
			// indexWriter.close();
		}
		LOGGER.info("Transfer " + count + " centroids");
	}

	@Override
	public int processForAllGroups(
			final CentroidProcessingFn<T> fn )
			throws IOException {
		List<String> centroidGroups;
		try {
			centroidGroups = getAllCentroidGroups();
		}
		catch (final IOException e) {
			throw new IOException(
					e);
		}

		int status = 0;
		for (final String groupID : centroidGroups) {
			status = fn.processGroup(
					groupID,
					getCentroidsForGroup(groupID));
			if (status != 0) {
				break;
			}
		}
		return status;
	}

	public static Collection<ParameterEnum<?>> getParameters() {
		return Arrays.asList(MY_PARAMS);
	}

	public static void setParameters(
			final Configuration config,
			final Class<?> scope,
			final PropertyManagement runTimeProperties ) {
		runTimeProperties.setConfig(
				MY_PARAMS,
				config,
				scope);
	}

	@Override
	public ByteArrayId getDataTypeId() {
		return new ByteArrayId(
				StringUtils.stringToBinary(centroidDataTypeId));
	}

	@Override
	public ByteArrayId getIndexId() {
		return index.getId();
	}

	public String getBatchId() {
		return this.batchId;
	}

	private DataAdapter<?> getAdapter() {
		return adapterStore.getAdapter(getDataTypeId());
	}

	private ToSimpleFeatureConverter<T> getFeatureConverter(
			final List<AnalyticItemWrapper<T>> items,
			final Class<? extends Geometry> shapeClass ) {
		final DataAdapter<?> adapter = getAdapter();
		return (adapter instanceof FeatureDataAdapter) ? new SimpleFeatureConverter(
				(FeatureDataAdapter) adapter,
				shapeClass) : new NonSimpleFeatureConverter(
				items.isEmpty() ? new String[0] : items.get(
						0).getExtraDimensions(),
				shapeClass);

	}

	private interface ToSimpleFeatureConverter<T>
	{
		SimpleFeatureType getFeatureType();

		SimpleFeature toSimpleFeature(
				AnalyticItemWrapper<T> item );
	}

	private static FeatureDataAdapter createFeatureAdapter(
			final SimpleFeatureType featureType,
			final Class<? extends Geometry> shapeClass ) {
		try {
			final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
			builder.setName(featureType.getName().getLocalPart());
			builder.setNamespaceURI(featureType.getName().getNamespaceURI());
			builder.setCRS(featureType.getCoordinateReferenceSystem());
			for (final AttributeDescriptor attr : featureType.getAttributeDescriptors()) {
				if (attr.getType() instanceof GeometryType) {
					builder.add(
							attr.getLocalName(),
							shapeClass);
				}
				else {
					builder.add(
							attr.getLocalName(),
							attr.getType().getBinding());
				}
			}
			return new FeatureDataAdapter(
					builder.buildFeatureType());
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Schema Creation Error.  Hint: Check the SRID.",
					e);
		}

		return null;
	}

	private static Geometry convert(
			final Geometry value,
			final Class<? extends Geometry> shapeClass ) {
		if (shapeClass.isInstance(value)) {
			return value;
		}
		if (shapeClass.isAssignableFrom(Point.class)) {
			return value.getCentroid();
		}
		final Geometry hull = value.convexHull();
		if (shapeClass.isInstance(hull)) {
			return hull;
		}
		return null;
	}

	private class SimpleFeatureConverter implements
			ToSimpleFeatureConverter<T>
	{

		final FeatureDataAdapter adapter;
		final Object[] defaults;
		final Class<? extends Geometry> shapeClass;

		public SimpleFeatureConverter(
				final FeatureDataAdapter adapter,
				final Class<? extends Geometry> shapeClass ) {
			this.adapter = createFeatureAdapter(
					adapter.getFeatureType(),
					shapeClass);
			int p = 0;
			this.shapeClass = shapeClass;
			final List<AttributeDescriptor> descriptors = adapter.getFeatureType().getAttributeDescriptors();
			defaults = new Object[descriptors.size()];
			for (final AttributeDescriptor descriptor : descriptors) {
				defaults[p++] = descriptor.getDefaultValue();
			}
		}

		@Override
		public SimpleFeatureType getFeatureType() {
			return adapter.getFeatureType();
		}

		@Override
		public SimpleFeature toSimpleFeature(
				final AnalyticItemWrapper<T> item ) {
			final SimpleFeature newFeature = SimpleFeatureBuilder.build(
					adapter.getFeatureType(),
					defaults,
					item.getID());
			int i = 0;
			for (final Object value : ((SimpleFeature) item.getWrappedItem()).getAttributes()) {
				if (value instanceof Geometry) {
					final Geometry newValue = convert(
							(Geometry) value,
							shapeClass);
					if (newValue == null) {
						return null;
					}
					newFeature.setAttribute(
							i++,
							newValue);
				}
				else {
					newFeature.setAttribute(
							i++,
							value);
				}
			}
			return newFeature;
		}
	}

	private class NonSimpleFeatureConverter implements
			ToSimpleFeatureConverter<T>
	{
		final SimpleFeatureType featureType;
		final Object[] defaults;
		final Class<? extends Geometry> shapeClass;

		public NonSimpleFeatureConverter(
				final String[] extraDimensionNames,
				final Class<? extends Geometry> shapeClass ) {
			featureType = AnalyticFeature.createFeatureAdapter(
					centroidDataTypeId,
					extraDimensionNames,
					BasicFeatureTypes.DEFAULT_NAMESPACE,
					ClusteringUtils.CLUSTERING_CRS,
					ClusterFeatureAttribute.values(),
					shapeClass).getFeatureType();
			this.shapeClass = shapeClass;
			final List<AttributeDescriptor> descriptors = featureType.getAttributeDescriptors();
			defaults = new Object[descriptors.size()];
			int p = 0;
			for (final AttributeDescriptor descriptor : descriptors) {
				defaults[p++] = descriptor.getDefaultValue();
			}
		}

		@Override
		public SimpleFeatureType getFeatureType() {
			return featureType;
		}

		@Override
		public SimpleFeature toSimpleFeature(
				final AnalyticItemWrapper<T> item ) {

			final Geometry value = item.getGeometry();
			final Geometry newValue = convert(
					value,
					shapeClass);
			if (newValue == null) {
				return null;
			}

			return AnalyticFeature.createGeometryFeature(
					featureType,
					item.getBatchID(),
					item.getID(),
					item.getName(),
					item.getGroupID(),
					item.getCost(),
					newValue,
					item.getExtraDimensions(),
					item.getDimensionValues(),
					item.getZoomLevel(),
					item.getIterationID(),
					item.getAssociationCount());
		}
	}

	public void toShapeFile(
			final String parentDir,
			final Class<? extends Geometry> shapeClass )
			throws IOException {
		// File shp = new File(parentDir + "/" + this.batchId + ".shp");
		// File shx = new File(parentDir + "/" + this.batchId + ".shx");
		final ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
		final Map<String, Serializable> params = new HashMap<String, Serializable>();
		try {
			params.put(
					"url",
					new URL(
							"file://" + parentDir + "/" + this.batchId + ".shp"));
		}
		catch (final MalformedURLException e) {
			LOGGER.error(
					"Error creating URL",
					e);
		}
		params.put(
				"create spatial index",
				Boolean.TRUE);

		final List<AnalyticItemWrapper<T>> centroids = loadCentroids(
				batchId,
				null);

		final ToSimpleFeatureConverter<T> converter = getFeatureConverter(
				centroids,
				shapeClass);

		final ShapefileDataStore newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
		newDataStore.createSchema(converter.getFeatureType());

		final Transaction transaction = new DefaultTransaction(
				"create");

		final String typeName = newDataStore.getTypeNames()[0];

		try (final FeatureWriter<SimpleFeatureType, SimpleFeature> writer = newDataStore.getFeatureWriterAppend(
				typeName,
				transaction)) {
			for (final AnalyticItemWrapper<T> item : centroids) {
				final SimpleFeature copy = writer.next();
				final SimpleFeature newFeature = converter.toSimpleFeature(item);
				for (final AttributeDescriptor attrD : newFeature.getFeatureType().getAttributeDescriptors()) {
					// the null case should only happen for geometry
					if (copy.getFeatureType().getDescriptor(
							attrD.getName()) != null) {
						copy.setAttribute(
								attrD.getName(),
								newFeature.getAttribute(attrD.getName()));
					}
				}
				// shape files force geometry name to be 'the_geom'. So isolate
				// this change
				copy.setDefaultGeometry(newFeature.getDefaultGeometry());
				writer.write();
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Problem with the FeatureWritter",
					e);
			transaction.rollback();
		}
		finally {
			transaction.commit();
			transaction.close();
		}
	}
}
