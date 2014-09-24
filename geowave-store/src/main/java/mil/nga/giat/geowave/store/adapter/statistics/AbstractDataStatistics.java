package mil.nga.giat.geowave.store.adapter.statistics;

import mil.nga.giat.geowave.index.ByteArrayId;

abstract public class AbstractDataStatistics<T> implements
		DataStatistics<T>
{
	protected ByteArrayId dataAdapterId;
	protected byte[] visibility;

	protected AbstractDataStatistics() {}

	public AbstractDataStatistics(
			final ByteArrayId dataAdapterId ) {
		this.dataAdapterId = dataAdapterId;
	}

	@Override
	public ByteArrayId getDataAdapterId() {
		return dataAdapterId;
	}

	@Override
	public void setDataAdapterId(
			final ByteArrayId dataAdapterId ) {
		this.dataAdapterId = dataAdapterId;
	}

	@Override
	public byte[] getVisibility() {
		return visibility;
	}

	@Override
	public void setVisibility(
			final byte[] visibility ) {
		this.visibility = visibility;
	}
}
