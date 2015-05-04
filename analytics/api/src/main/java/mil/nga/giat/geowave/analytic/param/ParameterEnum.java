package mil.nga.giat.geowave.analytic.param;

public interface ParameterEnum
{
	Class<?> getBaseClass();

	Enum<?> self();
}
