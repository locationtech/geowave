package mil.nga.giat.geowave.index;

public interface Mergeable extends
		Persistable
{
	public void merge(
			Mergeable merge );
}
