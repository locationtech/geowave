package mil.nga.giat.geowave.analytic.sample;

public interface SampleNotification<T>
{
	public void notify(
			T item,
			boolean partial );
}
