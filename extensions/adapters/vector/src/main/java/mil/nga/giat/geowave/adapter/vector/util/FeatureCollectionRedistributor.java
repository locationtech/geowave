package mil.nga.giat.geowave.adapter.vector.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import mil.nga.giat.geowave.adapter.vector.FeatureCollectionDataAdapter;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloIndexWriter;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.ArrayToElementsIterator;
import mil.nga.giat.geowave.datastore.accumulo.query.ElementsToArrayIterator;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.log4j.Logger;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.DefaultFeatureCollection;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class FeatureCollectionRedistributor
{

	private final static Logger LOGGER = Logger.getLogger(FeatureCollectionRedistributor.class);
	private final RedistributeConfig config;

	public FeatureCollectionRedistributor(
			final RedistributeConfig config ) {
		this.config = config;
	}

	public void redistribute()
			throws IOException,
			AccumuloSecurityException,
			AccumuloException,
			TableNotFoundException {

		final Index index = config.getIndexType().createDefaultIndex();

		final BasicAccumuloOperations operations = new BasicAccumuloOperations(
				config.getZookeeperUrl(),
				config.getInstanceName(),
				config.getUserName(),
				config.getPassword(),
				config.getTableNamespace());

		final FeatureCollectionDataAdapter adapter = new FeatureCollectionDataAdapter(
				config.getFeatureType(),
				config.getFeaturesPerEntry(),
				config.getCustomIndexHandlers(),
				config.getFieldVisiblityHandler());

		if (index.getIndexStrategy() instanceof HierarchicalNumericIndexStrategy) {
			final TieredSFCIndexStrategy tieredStrat = (TieredSFCIndexStrategy) index.getIndexStrategy();

			final String tablename = AccumuloUtils.getQualifiedTableName(
					config.getTableNamespace(),
					StringUtils.stringFromBinary(index.getId().getBytes()));

			// first, detach the transforming iterators
			removeIterators(
					tablename,
					operations.getConnector());

			final long startTime = new Date().getTime();

			final SubStrategy[] subStrategies = tieredStrat.getSubStrategies();

			// iterate over each tier
			for (int i = 0; i < subStrategies.length; i++) {

				final List<Integer> totalNumOverflow = Collections.synchronizedList(new ArrayList<Integer>());
				final List<Integer> totalNumOcuppiedSubTiles = Collections.synchronizedList(new ArrayList<Integer>());
				final List<Integer> totalNumCollsProcessed = Collections.synchronizedList(new ArrayList<Integer>());

				LOGGER.info("***   Processing Tier " + (i + 1) + " of " + subStrategies.length);

				final SubStrategy subStrat = subStrategies[i];

				// create an index for this substrategy
				final CustomIdIndex customIndex = new CustomIdIndex(
						subStrat.getIndexStrategy(),
						index.getIndexModel(),
						index.getId());

				final AccumuloConstraintsQuery q = new AccumuloConstraintsQuery(
						Arrays.asList(new ByteArrayId[] {
							adapter.getAdapterId()
						}),
						customIndex,
						new SpatialQuery(
								createBoundingBox(
										new double[] {
											0.0,
											0.0
										},
										360.0,
										180.0)).getIndexConstraints(customIndex.getIndexStrategy()),
						null);
				q.setQueryFiltersEnabled(false);

				// query at the specified index
				final CloseableIterator<DefaultFeatureCollection> itr = (CloseableIterator<DefaultFeatureCollection>) q.query(
						operations,
						new MemoryAdapterStore(
								new DataAdapter[] {
									adapter
								}),
						null);

				final ExecutorService executor = Executors.newFixedThreadPool(config.getNumThreads());
				final int subStratIdx = i;

				final ReentrantLock runLock = new ReentrantLock();
				runLock.lock();

				try {
					final ArrayBlockingQueue<DefaultFeatureCollection> queue = new ArrayBlockingQueue<DefaultFeatureCollection>(
							config.getNumThreads() * 2);

					// create numThreads consumers
					for (int thread = 0; thread < config.getNumThreads(); thread++) {
						executor.execute(new Runnable() {
							@Override
							public void run() {
								featCollConsumer(
										runLock,
										totalNumOverflow,
										totalNumOcuppiedSubTiles,
										totalNumCollsProcessed,
										queue,
										subStratIdx);
							}
						});
					}

					// iterate over each collection
					while (itr.hasNext()) {
						// create a new thread for each feature collection
						final DefaultFeatureCollection coll = itr.next();
						try {
							queue.put(coll);
						}
						catch (final InterruptedException e) {
							LOGGER.error(
									"Process interrupted while placing an item on the queue.",
									e);
						}
					}
				}
				finally {
					runLock.unlock();
				}

				try {
					executor.shutdown();
					executor.awaitTermination(
							Long.MAX_VALUE,
							TimeUnit.DAYS);
				}
				catch (final InterruptedException e) {
					LOGGER.error(
							"Process interrupted while awaiting executor termination.",
							e);
				}
				itr.close();

				LOGGER.info("***     Colls. Processed: " + listSum(totalNumCollsProcessed));
				LOGGER.info("***     Overflowed Tiles: " + listSum(totalNumOverflow));
				LOGGER.info("***     Occupied SubTiles: " + listSum(totalNumOcuppiedSubTiles));
			}

			final long stopTime = new Date().getTime();

			// finally, attach the transforming iterators
			attachIterators(
					index.getIndexModel(),
					tablename,
					operations.getConnector());

			LOGGER.info("*** Runtime: " + (stopTime - startTime) + " ms");
		}
	}

	private Integer listSum(
			final List<Integer> list ) {
		Integer sum = 0;
		for (final Integer i : list) {
			sum = sum + i;
		}
		return sum;
	}

	private void featCollConsumer(
			final ReentrantLock runLock,
			final List<Integer> totalNumOverflow,
			final List<Integer> totalNumOcuppiedSubTiles,
			final List<Integer> totalNumCollsProcessed,
			final ArrayBlockingQueue<DefaultFeatureCollection> queue,
			final int subStratIdx ) {

		int numOverflow = 0;
		int numOcuppiedSubTiles = 0;
		int numCollsProcessed = 0;

		final Instance inst = new ZooKeeperInstance(
				config.getInstanceName(),
				config.getZookeeperUrl());

		Connector connector = null;
		try {
			connector = inst.getConnector(
					config.getUserName(),
					config.getPassword());
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to create accumulo connector.",
					e);
		}

		final Index index;
		if (config.getTier() < 0) {
			index = config.getIndexType().createDefaultIndex();
		}
		else {
			final Index tempIdx = config.getIndexType().createDefaultIndex();
			final SubStrategy[] subStrats = ((TieredSFCIndexStrategy) tempIdx.getIndexStrategy()).getSubStrategies();
			index = new CustomIdIndex(
					subStrats[config.getTier()].getIndexStrategy(),
					tempIdx.getIndexModel(),
					tempIdx.getId());
		}

		final BasicAccumuloOperations featureCollectionOperations = new BasicAccumuloOperations(
				connector,
				config.getTableNamespace());

		final AccumuloDataStore featureCollectionDataStore = new AccumuloDataStore(
				featureCollectionOperations);

		final AccumuloIndexWriter featureCollectionWriter = new AccumuloIndexWriter(
				index,
				featureCollectionOperations,
				featureCollectionDataStore);

		final TieredSFCIndexStrategy tieredStrat = (TieredSFCIndexStrategy) index.getIndexStrategy();

		final FeatureDataAdapter featAdapter = new FeatureDataAdapter(
				config.getFeatureType());

		final RawFeatureCollectionDataAdapter featureCollectionAdapter = new RawFeatureCollectionDataAdapter(
				config.getFeatureType(),
				config.getFeaturesPerEntry(),
				config.getCustomIndexHandlers(),
				config.getFieldVisiblityHandler());

		final SubStrategy[] subStrategies = tieredStrat.getSubStrategies();

		while ((queue.size() > 0) || !runLock.tryLock()) {

			DefaultFeatureCollection featColl = null;
			try {
				featColl = queue.poll(
						100,
						TimeUnit.MILLISECONDS);
			}
			catch (final InterruptedException e) {
				LOGGER.error(
						"Process interrupted while polling queue.",
						e);
			}

			if (featColl != null) {

				numCollsProcessed++;

				// use the first feature to determine the index insertion id
				final SimpleFeatureIterator itr = featColl.features();
				final SimpleFeature sft = itr.next();
				itr.close();
				final MultiDimensionalNumericData bounds = featAdapter.encode(
						sft,
						index.getIndexModel()).getNumericData(
						index.getIndexModel().getDimensions());

				final List<ByteArrayId> ids = subStrategies[subStratIdx].getIndexStrategy().getInsertionIds(
						bounds);

				// TODO: This will need to be modified to support polygon
				// geometries
				if (ids.size() > 1) {
					LOGGER.warn("Multiple row ids returned for this entry.");
				}

				final ByteArrayId id = ids.get(0);

				boolean subTilesOccupied = false;
				final boolean tilespaceFull = featColl.size() > config.getFeaturesPerEntry();

				// for each tier below the current one, make sure there that
				// none of this tile's subtiles are populated
				if (!tilespaceFull) {
					for (int j = subStratIdx + 1; j < subStrategies.length; j++) {

						// create an index for this substrategy
						final CustomIdIndex subIndex = new CustomIdIndex(
								subStrategies[j].getIndexStrategy(),
								index.getIndexModel(),
								index.getId());

						// build the subtier query
						final AccumuloConstraintsQuery subQuery = new AccumuloConstraintsQuery(
								Arrays.asList(new ByteArrayId[] {
									featureCollectionAdapter.getAdapterId()
								}),
								subIndex,
								getSlimBounds(subStrategies[subStratIdx].getIndexStrategy().getRangeForId(
										id)),
								null);
						subQuery.setQueryFiltersEnabled(false);

						// query at the specified subtier
						final CloseableIterator<DefaultFeatureCollection> subItr = (CloseableIterator<DefaultFeatureCollection>) subQuery.query(
								featureCollectionOperations,
								new MemoryAdapterStore(
										new DataAdapter[] {
											featureCollectionAdapter
										}),
								null);

						// if there are any points, we need to set a flag to
						// move this collection to the next lowest tier
						if (subItr.hasNext()) {
							subTilesOccupied = true;
							numOcuppiedSubTiles++;
							try {
								subItr.close();
							}
							catch (final IOException e) {
								LOGGER.error(
										"Unable to close iterator.",
										e);
							}
							break;
						}
					}
				}
				else {
					numOverflow++;
				}

				// if the collection size is greater than tilesize
				// or there are points in the subtiles below this tile
				if (tilespaceFull || subTilesOccupied) {

					// build a row id for deletion
					final AccumuloRowId rowId = new AccumuloRowId(
							id.getBytes(),
							new byte[] {},
							featureCollectionAdapter.getAdapterId().getBytes(),
							-1);

					// delete this tile
					boolean result = featureCollectionOperations.delete(
							StringUtils.stringFromBinary(index.getId().getBytes()),
							new ByteArrayId(
									rowId.getRowId()),
							featureCollectionAdapter.getAdapterId().getString(),
							null);

					// if deletion failed, wait and try again a few times
					if (!result) {
						LOGGER.warn("Unable to delete row.  Trying again...");
						final int attempts = 5;
						for (int attempt = 0; attempt < attempts; attempt++) {

							try {
								Thread.sleep(500);
							}
							catch (final InterruptedException e) {
								LOGGER.error(
										"Sleep interrupted!",
										e);
							}

							result = featureCollectionOperations.delete(
									StringUtils.stringFromBinary(index.getId().getBytes()),
									new ByteArrayId(
											rowId.getRowId()),
									featureCollectionAdapter.getAdapterId().getString(),
									null);
						}
						if (!result) {
							LOGGER.error("After " + attempts + " attempts, Index Id: [" + Arrays.toString(rowId.getInsertionId()) + "] was NOT deleted successfully!");
						}
						else {
							LOGGER.info("The row was deleted successfully!");
						}
					}

					// if the deletion was unsuccessful, don't re-ingest
					// anything
					if (result) {
						if (subTilesOccupied) {
							// re-ingest the data if the tiers below this one
							// are occupied. send the tier information along
							// with our data
							featureCollectionWriter.write(
									featureCollectionAdapter,
									new FitToIndexDefaultFeatureCollection(
											featColl,
											id,
											subStratIdx));
						}
						else {
							// re-ingest because the tile has overflowed
							featureCollectionWriter.write(
									featureCollectionAdapter,
									featColl);
						}
					}
				}
			}
		}

		runLock.unlock();

		totalNumOverflow.add(numOverflow);
		totalNumOcuppiedSubTiles.add(numOcuppiedSubTiles);
		totalNumCollsProcessed.add(numCollsProcessed);

		featureCollectionWriter.close();
	}

	private static class RawFeatureCollectionDataAdapter extends
			FeatureCollectionDataAdapter
	{
		public RawFeatureCollectionDataAdapter(
				final SimpleFeatureType type,
				final int featuresPerEntry,
				final List<PersistentIndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>> customIndexHandlers,
				final FieldVisibilityHandler<DefaultFeatureCollection, Object> fieldVisiblityHandler ) {
			super(
					type,
					featuresPerEntry,
					customIndexHandlers,
					fieldVisiblityHandler);
		}

		@Override
		public IteratorConfig[] getAttachedIteratorConfig(
				final Index index ) {
			return null;
		}
	}

	private MultiDimensionalNumericData getSlimBounds(
			final MultiDimensionalNumericData bounds ) {
		// Ideally: smallest dimension range / (4*max bins per dimension {i.e.
		// 2^31})
		final double epsilon = 180.0 / (4.0 * Math.pow(
				2.0,
				31.0));

		final NumericData[] slimRanges = new NumericData[2];
		slimRanges[0] = new NumericRange(
				bounds.getDataPerDimension()[0].getMin() + epsilon,
				bounds.getDataPerDimension()[0].getMax() - epsilon);
		slimRanges[1] = new NumericRange(
				bounds.getDataPerDimension()[1].getMin() + epsilon,
				bounds.getDataPerDimension()[1].getMax() - epsilon);

		return new BasicNumericDataset(
				slimRanges);
	}

	private Geometry createBoundingBox(
			final double[] centroid,
			final double width,
			final double height ) {

		final double north = centroid[1] + (height / 2.0);
		final double south = centroid[1] - (height / 2.0);
		final double east = centroid[0] + (width / 2.0);
		final double west = centroid[0] - (width / 2.0);

		final Coordinate[] coordArray = new Coordinate[5];
		coordArray[0] = new Coordinate(
				west,
				south);
		coordArray[1] = new Coordinate(
				east,
				south);
		coordArray[2] = new Coordinate(
				east,
				north);
		coordArray[3] = new Coordinate(
				west,
				north);
		coordArray[4] = new Coordinate(
				west,
				south);

		return new GeometryFactory().createPolygon(coordArray);
	}

	private void removeIterators(
			final String tablename,
			final Connector connector )
			throws AccumuloSecurityException,
			AccumuloException,
			TableNotFoundException {
		connector.tableOperations().removeIterator(
				tablename,
				new IteratorSetting(
						FeatureCollectionDataAdapter.ARRAY_TO_ELEMENTS_PRIORITY,
						ArrayToElementsIterator.class).getName(),
				EnumSet.of(IteratorScope.scan));

		connector.tableOperations().removeIterator(
				tablename,
				new IteratorSetting(
						FeatureCollectionDataAdapter.ELEMENTS_TO_ARRAY_PRIORITY,
						ElementsToArrayIterator.class).getName(),
				EnumSet.of(IteratorScope.scan));
	}

	private void attachIterators(
			final CommonIndexModel indexModel,
			final String tablename,
			final Connector connector )
			throws AccumuloSecurityException,
			AccumuloException,
			TableNotFoundException {
		final String modelString = ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(indexModel));
		final IteratorSetting decompSetting = new IteratorSetting(
				FeatureCollectionDataAdapter.ARRAY_TO_ELEMENTS_PRIORITY,
				ArrayToElementsIterator.class);
		decompSetting.addOption(
				ArrayToElementsIterator.MODEL,
				modelString);
		decompSetting.addOption(
				TransformingIterator.MAX_BUFFER_SIZE_OPT,
				Integer.toString(512000000));
		connector.tableOperations().attachIterator(
				tablename,
				decompSetting,
				EnumSet.of(IteratorScope.scan));

		final IteratorSetting builderSetting = new IteratorSetting(
				FeatureCollectionDataAdapter.ELEMENTS_TO_ARRAY_PRIORITY,
				ElementsToArrayIterator.class);
		builderSetting.addOption(
				ElementsToArrayIterator.MODEL,
				modelString);
		builderSetting.addOption(
				TransformingIterator.MAX_BUFFER_SIZE_OPT,
				Integer.toString(512000000));
		connector.tableOperations().attachIterator(
				tablename,
				builderSetting,
				EnumSet.of(IteratorScope.scan));
	}

}
