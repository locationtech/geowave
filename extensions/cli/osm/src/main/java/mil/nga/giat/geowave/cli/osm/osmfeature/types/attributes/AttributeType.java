package mil.nga.giat.geowave.cli.osm.osmfeature.types.attributes;

public interface AttributeType<T>
{
	public T convert(
			Object source );

	public Class getClassType();
}
