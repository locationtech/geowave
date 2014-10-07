package mil.nga.giat.geowave.store.query;

import java.nio.ByteBuffer;
import java.util.Date;

import mil.nga.giat.geowave.index.sfc.data.NumericData;

public class TemporalRange  {
	private Date startTime;
	private Date endTime;

	public static final Date START_TIME = new Date(0);
	public static final Date END_TIME = new Date(Long.MAX_VALUE);

	protected TemporalRange() {
		this.startTime = START_TIME;
		this.endTime = END_TIME;
	}
	
	public TemporalRange(final Date startTime, final Date endTime) {
		super();
		this.startTime = startTime;
		this.endTime = endTime;
	}

	public Date getStartTime() {
		return startTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public boolean isWithin(Date time) {
		return (startTime.before(time) || startTime.equals(time))
				&& (endTime.equals(time) || endTime.after(time));
	}

	public boolean isWithin(NumericData timeRange) {
		final double st = startTime.getTime();
		final double et = endTime.getTime();
		final double rst = timeRange.getMin();
		final double ret = timeRange.getMax();
		return ((st < rst && et > rst) || (st < ret && et > ret) || (st < rst && et > ret));
	}

	public byte[] toBinary() {
		ByteBuffer buf = ByteBuffer.allocate(16);
		buf.putLong(startTime.getTime());
		buf.putLong(endTime.getTime());
		return buf.array();
	}
	
	public void  fromBinary( byte[] data) {
		ByteBuffer buf = ByteBuffer.wrap(data);
		startTime = new Date(buf.getLong());
		endTime = new Date(buf.getLong());
	}
	
	
	@Override
	public String toString() {
		return "TemporalRange [startTime=" + startTime + ", endTime=" + endTime
				+ "]";
	}

	protected static  final int  getBufferSize() {
		return 16;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((endTime == null) ? 0 : endTime.hashCode());
		result = prime * result
				+ ((startTime == null) ? 0 : startTime.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TemporalRange other = (TemporalRange) obj;
		if (endTime == null) {
			if (other.endTime != null)
				return false;
		} else if (!endTime.equals(other.endTime))
			return false;
		if (startTime == null) {
			if (other.startTime != null)
				return false;
		} else if (!startTime.equals(other.startTime))
			return false;
		return true;
	}

}
