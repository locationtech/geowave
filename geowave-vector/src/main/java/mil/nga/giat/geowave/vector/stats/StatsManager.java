package mil.nga.giat.geowave.vector.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.TimeUtils;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsVisibilityHandler;
import mil.nga.giat.geowave.store.adapter.statistics.FieldIdStatisticVisibility;
import mil.nga.giat.geowave.store.adapter.statistics.FieldTypeStatisticVisibility;
import mil.nga.giat.geowave.store.dimension.GeometryWrapper;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Geometry;

public class StatsManager
{

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
		return null;
	}

	public DataStatisticsVisibilityHandler<SimpleFeature> getVisibilityHandler(
			final ByteArrayId statisticsId ) {
		if (statisticsId.equals(CountDataStatistics.STATS_ID)) {
			return GEOMETRY_VISIBILITY_HANDLER;
		}
		return visibilityHandlers.get(statisticsId);
	}

	public StatsManager(
			final DataAdapter<SimpleFeature> dataAdapter,
			final SimpleFeatureType feature ) {
		for (final AttributeDescriptor descriptor : feature.getAttributeDescriptors()) {
			if (TimeUtils.isTemporal(descriptor.getType().getBinding())) {
				statsList.add(new FeatureTimeRangeStatistics(
						dataAdapter.getAdapterId(),
						descriptor.getLocalName()));
			}
			else if (Number.class.isAssignableFrom(descriptor.getType().getBinding())) {
				statsList.add(new FeatureNumericRangeStatistics(
						dataAdapter.getAdapterId(),
						descriptor.getLocalName()));
			}
			else if (Geometry.class.isAssignableFrom(descriptor.getType().getBinding())) {
				statsList.add(new FeatureBoundingBoxStatistics(
						dataAdapter.getAdapterId(),
						descriptor.getLocalName()));
			}
			else continue;
			// last one added to set visibility
			visibilityHandlers.put(
					statsList.get(statsList.size() - 1).getStatisticsId(),
					new FieldIdStatisticVisibility(
							new ByteArrayId(
									descriptor.getLocalName())));
		}
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
