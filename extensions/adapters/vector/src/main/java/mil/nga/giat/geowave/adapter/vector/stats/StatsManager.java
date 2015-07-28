package mil.nga.giat.geowave.adapter.vector.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.TimeUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.statistics.FieldIdStatisticVisibility;
import mil.nga.giat.geowave.core.store.adapter.statistics.FieldTypeStatisticVisibility;

import org.apache.log4j.Logger;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Geometry;

public class StatsManager
{

	private final static Logger LOGGER = Logger.getLogger(StatsManager.class);
	private final static DataStatisticsVisibilityHandler<SimpleFeature> GEOMETRY_VISIBILITY_HANDLER = new FieldTypeStatisticVisibility<SimpleFeature>(
			GeometryWrapper.class);

	private final List<DataStatistics<SimpleFeature>> statsList = new ArrayList<DataStatistics<SimpleFeature>>();
	private final Map<ByteArrayId, DataStatisticsVisibilityHandler<SimpleFeature>> visibilityHandlers = new HashMap<ByteArrayId, DataStatisticsVisibilityHandler<SimpleFeature>>();

	public DataStatistics<SimpleFeature> createDataStatistics(
			final DataAdapter<SimpleFeature> dataAdapter,
			final ByteArrayId statisticsId ) {
		for (final DataStatistics<SimpleFeature> stat : statsList) {
			if (stat.getStatisticsId().equals(
					statisticsId)) {
				return ((AbstractDataStatistics<SimpleFeature>) stat).duplicate();
			}
		}
		if (statisticsId.equals(CountDataStatistics.STATS_ID)) {
			return new CountDataStatistics<SimpleFeature>(
					dataAdapter.getAdapterId());
		}
		LOGGER.warn("Unrecognized statistics ID " + statisticsId.getString() + " using count statistic");
		return new CountDataStatistics<SimpleFeature>(
				dataAdapter.getAdapterId(),
				statisticsId);
	}

	public DataStatisticsVisibilityHandler<SimpleFeature> getVisibilityHandler(
			final ByteArrayId statisticsId ) {
		if (statisticsId.equals(CountDataStatistics.STATS_ID) || !visibilityHandlers.containsKey(statisticsId)) {
			return GEOMETRY_VISIBILITY_HANDLER;
		}
		return visibilityHandlers.get(statisticsId);
	}

	public StatsManager(
			final DataAdapter<SimpleFeature> dataAdapter,
			final SimpleFeatureType persistedType ) {
		this(
				dataAdapter,
				persistedType,
				null,
				null);
	}

	public StatsManager(
			final DataAdapter<SimpleFeature> dataAdapter,
			final SimpleFeatureType persistedType,
			final SimpleFeatureType reprojectedType,
			final MathTransform transform ) {
		for (final AttributeDescriptor descriptor : persistedType.getAttributeDescriptors()) {
			// for temporal and geometry because there is a dependency on these
			// stats
			// for optimizations within the GeoServer adapter.
			if (TimeUtils.isTemporal(descriptor.getType().getBinding())) {
				addStats(
						new FeatureTimeRangeStatistics(
								dataAdapter.getAdapterId(),
								descriptor.getLocalName()),
						new FieldIdStatisticVisibility(
								new ByteArrayId(
										descriptor.getLocalName())));
			}
			else if (Geometry.class.isAssignableFrom(descriptor.getType().getBinding())) {
				addStats(
						new FeatureBoundingBoxStatistics(
								dataAdapter.getAdapterId(),
								descriptor.getLocalName(),
								persistedType,
								reprojectedType,
								transform),
						new FieldIdStatisticVisibility(
								new ByteArrayId(
										descriptor.getLocalName())));
			}

			if (descriptor.getUserData().containsKey(
					"stats")) {
				final StatsConfigurationCollection statsConfigurations = (StatsConfigurationCollection) descriptor.getUserData().get(
						"stats");
				for (StatsConfig<SimpleFeature> statConfig : statsConfigurations.getConfigurationsForAttribute()) {
					addStats(
							statConfig.create(
									dataAdapter.getAdapterId(),
									descriptor.getLocalName()),
							new FieldIdStatisticVisibility(
									new ByteArrayId(
											descriptor.getLocalName())));
				}

			}
			else if (Number.class.isAssignableFrom(descriptor.getType().getBinding())) {
				addStats(
						new FeatureNumericRangeStatistics(
								dataAdapter.getAdapterId(),
								descriptor.getLocalName()),
						new FieldIdStatisticVisibility(
								new ByteArrayId(
										descriptor.getLocalName())));

				addStats(
						new FeatureFixedBinNumericStatistics(
								dataAdapter.getAdapterId(),
								descriptor.getLocalName()),
						new FieldIdStatisticVisibility(
								new ByteArrayId(
										descriptor.getLocalName())));

			}
			else if (String.class.isAssignableFrom(descriptor.getType().getBinding())) {
				addStats(
						new FeatureHyperLogLogStatistics(
								dataAdapter.getAdapterId(),
								descriptor.getLocalName(),
								16),
						new FieldIdStatisticVisibility(
								new ByteArrayId(
										descriptor.getLocalName())));
			}

		}
	}

	/**
	 * Supports replacement.
	 * 
	 * @param stats
	 * @param visibilityHandler
	 */
	public void addStats(
			DataStatistics<SimpleFeature> stats,
			DataStatisticsVisibilityHandler<SimpleFeature> visibilityHandler ) {
		int replaceStat = 0;
		for (DataStatistics<SimpleFeature> currentStat : statsList) {
			if (currentStat.getStatisticsId().equals(
					stats.getStatisticsId())) {
				break;
			}
			replaceStat++;
		}
		if (replaceStat < statsList.size()) this.statsList.remove(replaceStat);
		this.statsList.add(stats);
		this.visibilityHandlers.put(
				stats.getStatisticsId(),
				visibilityHandler);
	}

	public ByteArrayId[] getSupportedStatisticsIds() {
		final ByteArrayId[] statsIds = new ByteArrayId[statsList.size() + 1];
		int i = 0;
		for (final DataStatistics<SimpleFeature> stat : statsList) {
			statsIds[i++] = stat.getStatisticsId();
		}
		statsIds[i] = CountDataStatistics.STATS_ID;
		return statsIds;
	}
}
