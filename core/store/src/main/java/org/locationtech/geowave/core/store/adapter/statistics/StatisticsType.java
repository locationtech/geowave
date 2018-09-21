package org.locationtech.geowave.core.store.adapter.statistics;

import org.locationtech.geowave.core.index.ByteArrayId;

/**
 * This is a marker class extending ByteArrayId that additionally provides type
 * checking with a generic.
 *
 * @param <R>
 *            The type of statistic
 */
public class StatisticsType<R> extends
		ByteArrayId
{
	private static final long serialVersionUID = 1L;

	public StatisticsType() {
		super();
	}

	public StatisticsType(
			byte[] id ) {
		super(
				id);
	}

	public StatisticsType(
			String id ) {
		super(
				id);
	}

}
