package mil.nga.giat.geowave.analytics.mapreduce.kde.parser;

public interface CustomFunction
{
	public Object evaluate(
			Object[] parameters );

	public String getName();
}
