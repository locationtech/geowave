package mil.nga.giat.geowave.analytics.sample;

public interface SampleNotification<T>
{
	public void notify(
			T item,
			boolean partial );
}
