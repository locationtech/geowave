package mil.nga.giat.geowave.adapter.vector.stats;

import java.io.Serializable;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

import org.codehaus.jackson.annotate.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public interface StatsConfig<T> extends
		Serializable
{
	DataStatistics<T> create(
			final ByteArrayId dataAdapterId,
			final String fieldName );
}
