package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;

abstract public class AbstractDataStatistics<T> implements
		DataStatistics<T>
{
	protected ByteArrayId dataAdapterId;
	protected byte[] visibility;
	protected ByteArrayId statisticsId;

	protected AbstractDataStatistics() {}

	public AbstractDataStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId statisticsId ) {
		this.dataAdapterId = dataAdapterId;
		this.statisticsId = statisticsId;
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

	@Override
	public ByteArrayId getStatisticsId() {
		return statisticsId;
	}

	protected ByteBuffer binaryBuffer(
			final int size ) {
		final byte aidBytes[] = dataAdapterId.getBytes();
		final byte sidBytes[] = statisticsId.getBytes();
		final ByteBuffer buffer = ByteBuffer.allocate(size + 8 + sidBytes.length + aidBytes.length);
		buffer.putInt(aidBytes.length);
		buffer.putInt(sidBytes.length);
		buffer.put(aidBytes);
		buffer.put(sidBytes);

		return buffer;
	}

	protected ByteBuffer binaryBuffer(
			final byte[] bytes ) {

		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		final int alen = buffer.getInt();
		final byte aidBytes[] = new byte[alen];
		final int slen = buffer.getInt();
		final byte sidBytes[] = new byte[slen];

		buffer.get(aidBytes);
		dataAdapterId = new ByteArrayId(
				aidBytes);
		buffer.get(sidBytes);
		statisticsId = new ByteArrayId(
				sidBytes);
		return buffer;
	}

	protected static ByteArrayId composeId(
			final String statsType,
			final String name ) {
		return new ByteArrayId(
				statsType + "#" + name);
	}

	protected static String decomposeNameFromId(
			final ByteArrayId id ) {
		final String idString = id.getString();
		final int pos = idString.lastIndexOf('#');
		return idString.substring(pos + 1);
	}

	@SuppressWarnings("unchecked")
	public DataStatistics<T> duplicate() {
		DataStatistics<T> newStats;
		try {
			newStats = this.getClass().newInstance();
		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(
					"Cannot duplicate statistics class " + this.getClass(),
					e);
		}

		newStats.fromBinary(toBinary());
		return newStats;
	}

	@Override
	public String toString() {
		return "AbstractDataStatistics [dataAdapterId=" + dataAdapterId.getString() + ", statisticsId=" + statisticsId.getString() + "]";
	}
}
