package mil.nga.giat.geowave.analytics.parameters;

public interface ParameterEnum
{
	Class<?> getBaseClass();
	Enum<?> self();
}
