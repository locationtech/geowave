package mil.nga.giat.geowave.core.index;

public interface Mergeable extends
		Persistable
{
	public void merge(
			Mergeable merge );
}
