package mil.nga.giat.geowave.store.query;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class TemporalConstraints {
	LinkedList<TemporalRange> constraints = new LinkedList<TemporalRange>();

	private static final TemporalRange FULL_RANGE = new TemporalRange(
			TemporalRange.START_TIME, TemporalRange.END_TIME);

	public TemporalConstraints() {

	}

	public void add(TemporalRange range) {
		int pos = 0;
		TemporalRange nextNeighbor = null;
		for (TemporalRange aRange : constraints) {
			nextNeighbor = aRange;
			if (nextNeighbor.getStartTime().after(range.getStartTime())) {
				break;
			} else if (nextNeighbor.getEndTime().after(range.getStartTime())
					|| nextNeighbor.getEndTime().equals(range.getStartTime())) {
				if (range.getEndTime().before(nextNeighbor.getEndTime())) {
					// subsummed
					return;
				} else {
					// replaced with larger range
					constraints.set(pos,
							new TemporalRange(nextNeighbor.getStartTime(),
									range.getEndTime()));
					return;
				}
			}
			pos++;
		}
		if (nextNeighbor != null
				&& nextNeighbor.getStartTime().before(range.getEndTime()))
			constraints
					.add(pos,
							new TemporalRange(range.getStartTime(),
									TemporalConstraints.max(
											nextNeighbor.getEndTime(),
											range.getEndTime())));
		else
			constraints.add(pos, range);
	}

	public static final Date max(Date one, Date two) {
		return one.before(two) ? two : one;
	}

	public static final Date min(Date one, Date two) {
		return one.before(two) ? one : two;
	}

	public Date getMinOr(Date min) {
		return (constraints.isEmpty()) ? min : constraints.getFirst()
				.getStartTime();
	}

	public Date getMaxOr(Date max) {
		return (constraints.isEmpty()) ? max : constraints.getLast()
				.getEndTime();
	}

	public boolean isEmpty() {
		return this.constraints.isEmpty();
	}

	public TemporalRange getEndRange() {
		return (this.constraints.isEmpty()) ? FULL_RANGE : this.constraints
				.getLast();
	}

	public TemporalRange getStartRange() {
		return (this.constraints.isEmpty()) ? FULL_RANGE : this.constraints
				.getFirst();
	}

	public List<TemporalRange> getRanges() {
		return this.constraints;
	}

	public static final TemporalConstraints findIntersections(
			TemporalConstraints sideL, TemporalConstraints sideR) {

		// seems odd. Means that one or the other side did not have time
		// constraints
		if (sideL.constraints.isEmpty())
			return sideR;
		if (sideR.constraints.isEmpty())
			return sideL;

		TemporalConstraints newSet = new TemporalConstraints();

		for (TemporalRange lRange : sideL.constraints) {
			for (TemporalRange rRange : sideR.constraints) {
				if (lRange.getEndTime().before(rRange.getStartTime())
						|| rRange.getEndTime().before(lRange.getStartTime()))
					continue;
				newSet.add(new TemporalRange(max(lRange.getStartTime(),
						rRange.getStartTime()), min(lRange.getEndTime(),
						rRange.getEndTime())));
			}
		}
		return newSet;
	}

	public static final TemporalConstraints merge(TemporalConstraints left,
			TemporalConstraints right) {
		if (!left.isEmpty())
			return right;
		if (!right.isEmpty())
			return left;

		TemporalConstraints newSetOfRanges = new TemporalConstraints();
		newSetOfRanges.constraints.addAll(left.constraints);
		for (TemporalRange range : right.constraints) {
			newSetOfRanges.add(range);
		}
		return newSetOfRanges;
	}

	public byte[] toBinary() {
		ByteBuffer buffer = ByteBuffer
				.allocate(4 + (constraints.size() * TemporalRange
						.getBufferSize()));
		buffer.putInt(constraints.size());

		for (TemporalRange range : this.constraints) {
			buffer.put(range.toBinary());
		}

		return buffer.array();
	}

	public void fromBinary(byte[] data) {
		ByteBuffer buffer = ByteBuffer.wrap(data);

		int s = buffer.getInt();
		byte[] rangeBuf = new byte[TemporalRange.getBufferSize()];
		for (int i = 0; i < s; i++) {
			buffer.get(rangeBuf);
			TemporalRange range = new TemporalRange();
			range.fromBinary(rangeBuf);
			this.add(range);
		}

	}

	@Override
	public String toString() {
		return "TemporalConstraints [constraints=" + constraints + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((constraints == null) ? 0 : constraints.hashCode());
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
		TemporalConstraints other = (TemporalConstraints) obj;
		if (constraints == null) {
			if (other.constraints != null)
				return false;
		} else if (!constraints.equals(other.constraints))
			return false;
		return true;
	}

}
