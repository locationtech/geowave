package mil.nga.giat.geowave.adapter.vector.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.geotime.TimeUtils;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.FieldIdStatisticVisibility;
import mil.nga.giat.geowave.core.store.adapter.statistics.FieldTypeStatisticVisibility;

/**
 * Object that manages statistics for an adapter
 */

public class StatsManager
{

	private final static Logger LOGGER = LoggerFactory.getLogger(StatsManager.class);

	/**
	 * Visibility that can be used within GeoWave as a CommonIndexValue
	 */
	private final static EntryVisibilityHandler<SimpleFeature> GEOMETRY_VISIBILITY_HANDLER = new FieldTypeStatisticVisibility<SimpleFeature>(
			GeometryWrapper.class);

	/**
	 * List of stats objects supported by this manager for the adapter
	 */

	private final List<DataStatistics<SimpleFeature>> statsObjList = new ArrayList<DataStatistics<SimpleFeature>>();
	/**
	 * List of visibility handlers supported by this manager for the stats
	 * objects
	 */

	private final Map<ByteArrayId, EntryVisibilityHandler<SimpleFeature>> visibilityHandlers = new HashMap<ByteArrayId, EntryVisibilityHandler<SimpleFeature>>();

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Constructor - Creates a StatsManager that supports the specified adapter
	 * with a persisted SimpleFeatureType
	 * 
	 * @param dataAdapter
	 *            - adapter to be associated with this manager
	 * @param persistedType
	 *            - the feature type to be associated with the given adapter
	 */

	public StatsManager(
			final DataAdapter<SimpleFeature> dataAdapter,
			final SimpleFeatureType persistedType ) {
		this(
				dataAdapter,
				persistedType,
				null,
				null);
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Constructor - Creates a StatsManager that supports the specified adapter
	 * with a persisted and re-projected SimpleFeatureType that is translated by
	 * the transform
	 * 
	 * @param dataAdapter
	 *            - adapter to be associated with this manager
	 * @param persistedType
	 *            - the feature type to be associated with the given adapter
	 * @param reprojectedType
	 *            - feature type re-projected with new CRS. Used if stats object
	 *            is a Feature Bounding Box.
	 * @param transform
	 *            - type of transform applied to persisted type. Used if stats
	 *            object is a Feature Bounding Box.
	 */

	public StatsManager(
			final DataAdapter<SimpleFeature> dataAdapter,
			final SimpleFeatureType persistedType,
			final SimpleFeatureType reprojectedType,
			final MathTransform transform ) {
		// For all the attribute descriptors listed in the persisted
		// featuretype,
		// go through and look for stats that can be tracked against this
		// attribute type

		for (final AttributeDescriptor descriptor : persistedType.getAttributeDescriptors()) {

			String fieldName = descriptor.getLocalName();
			FieldIdStatisticVisibility vis = new FieldIdStatisticVisibility(
					new ByteArrayId(
							fieldName));
			ByteArrayId adapterID = dataAdapter.getAdapterId();

			// ---------------------------------------------------------------------
			// For temporal and geometry because there is a dependency on these
			// stats for optimizations within the GeoServer adapter.

			if (TimeUtils.isTemporal(descriptor.getType().getBinding())) {
				FeatureTimeRangeStatistics statObj = new FeatureTimeRangeStatistics(
						adapterID,
						fieldName);

				addStats(
						statObj,
						vis);
			}

			else if (Geometry.class.isAssignableFrom(descriptor.getType().getBinding())) {
				FeatureBoundingBoxStatistics statObj = new FeatureBoundingBoxStatistics(
						adapterID,
						fieldName,
						persistedType,
						reprojectedType,
						transform);
				addStats(
						statObj,
						vis);
			}

			// ---------------------------------------------------------------------
			// If there is a stats field in the feature, then that is a
			// StatsConfigurationCollection and the stats objects for the
			// feature need to be extracted and added as stats for this adapter

			if (descriptor.getUserData().containsKey(
					"stats")) {
				final StatsConfigurationCollection statsConfigurations = (StatsConfigurationCollection) descriptor
						.getUserData()
						.get(
								"stats");

				List<StatsConfig<SimpleFeature>> featureConfigs = statsConfigurations.getConfigurationsForAttribute();

				for (StatsConfig<SimpleFeature> statConfig : featureConfigs) {
					addStats(
							statConfig.create(
									adapterID,
									fieldName),
							vis);
				}

			}

			// ---------------------------------------------------------------------
			// If this numeric, then add two stats objects that relate

			else if (Number.class.isAssignableFrom(descriptor.getType().getBinding())) {
				addStats(
						new FeatureNumericRangeStatistics(
								adapterID,
								fieldName),
						vis);
				addStats(
						new FeatureFixedBinNumericStatistics(
								adapterID,
								fieldName),
						vis);
			}
		}
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Creates a stats object to be tracked for this adapter based on type
	 * 
	 * @param dataAdapter
	 *            - adapter to be associated with this manager
	 * @param statisticsId
	 *            - name of statistics type to be created
	 * 
	 * @return new statistics object of specified type
	 */

	public DataStatistics<SimpleFeature> createDataStatistics(
			final DataAdapter<SimpleFeature> dataAdapter,
			final ByteArrayId statisticsId ) {
		for (final DataStatistics<SimpleFeature> statObj : statsObjList) {
			if (statObj.getStatisticsId().equals(
					statisticsId)) {
				// TODO most of the data statistics seem to do shallow clones
				// that pass along a lot of references - this seems
				// counter-intuitive to the spirit of a "create" method, but it
				// seems to work right now?
				return ((AbstractDataStatistics<SimpleFeature>) statObj).duplicate();
			}
			// LOGGER.warn("Comparing ID '" + statisticsId.getString() + "' and
			// '" + stat.getStatisticsId().getString() + "'");
		}

		if (statisticsId.getString().equals(
				CountDataStatistics.STATS_TYPE.getString())) {
			return new CountDataStatistics<SimpleFeature>(
					dataAdapter.getAdapterId());
		}

		// HP Fortify "Log Forging" false positive
		// What Fortify considers "user input" comes only
		// from users with OS-level access anyway

		LOGGER.warn("Unrecognized statistics ID " + statisticsId.getString() + ", using count statistic.");
		return new CountDataStatistics<SimpleFeature>(
				dataAdapter.getAdapterId(),
				statisticsId);
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Get a visibility handler for the specified statistics object
	 * 
	 * @param statisticsId
	 * @return - visibility handler for given stats object
	 */

	public EntryVisibilityHandler<SimpleFeature> getVisibilityHandler(
			final ByteArrayId statisticsId ) {
		// If the statistics object is of type CountDataStats or there is no
		// visibility handler, then return the default visibility handler

		if (statisticsId.equals(CountDataStatistics.STATS_TYPE) || (!visibilityHandlers.containsKey(statisticsId))) {
			return GEOMETRY_VISIBILITY_HANDLER;
		}

		return visibilityHandlers.get(statisticsId);
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Adds/Replaces a stats object for the given adapter <br>
	 * Supports object replacement.
	 * 
	 * @param statsObj
	 *            - data stats object to be tracked by adding or replacement
	 * @param visibilityHandler
	 *            - type of visibility required to access the stats object
	 * 
	 */

	public void addStats(
			DataStatistics<SimpleFeature> statsObj,
			EntryVisibilityHandler<SimpleFeature> visibilityHandler ) {
		int replaceStat = 0;

		// Go through stats list managed by this manager and look for a match
		for (DataStatistics<SimpleFeature> currentStat : statsObjList) {
			if (currentStat.getStatisticsId().equals(
					statsObj.getStatisticsId())) {
				// If a match was found for an existing stat object in list,
				// remove it now and replace it later.
				this.statsObjList.remove(replaceStat);
				break;
			}
			replaceStat++; // Not found, check next stat object
		}

		this.statsObjList.add(statsObj);
		this.visibilityHandlers.put(
				statsObj.getStatisticsId(),
				visibilityHandler);
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Get an array of stats object IDs for the Stats Manager
	 * 
	 * @return Array of stats object IDs as 'ByteArrayId'
	 */

	public ByteArrayId[] getSupportedStatisticsIds() {
		// Why are we adding a CountDataStatistics??

		final ByteArrayId[] statObjIds = new ByteArrayId[statsObjList.size() + 1];
		int i = 0;

		for (final DataStatistics<SimpleFeature> statObj : statsObjList) {
			statObjIds[i++] = statObj.getStatisticsId();
		}

		statObjIds[i] = CountDataStatistics.STATS_TYPE;

		return statObjIds;
	}
}
