package mil.nga.giat.geowave.core.store.spi;

public interface ClassLoaderTransformerSpi
{
	public ClassLoader transform(
			ClassLoader classLoader );
}
