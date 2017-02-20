package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/***
 * Defines a unit interval on a number line
 *
 */
public class ByteArrayRange implements
		Comparable<ByteArrayRange>
{
	protected ByteArrayId start;
	protected ByteArrayId end;
	protected boolean singleValue;

	/***
	 *
	 * @param start
	 *            start of unit interval
	 * @param end
	 *            end of unit interval
	 */
	public ByteArrayRange(
			final ByteArrayId start,
			final ByteArrayId end ) {
		this(
				start,
				end,
				false);
	}

	/***
	 *
	 * @param start
	 *            start of unit interval
	 * @param end
	 *            end of unit interval
	 */
	public ByteArrayRange(
			final ByteArrayId start,
			final ByteArrayId end,
			final boolean singleValue ) {
		this.start = start;
		this.end = end;
		this.singleValue = singleValue;
	}

	public ByteArrayId getStart() {
		return start;
	}

	public ByteArrayId getEnd() {
		return end;
	}

	public ByteArrayId getEndAsNextPrefix() {
		return new ByteArrayId(
				end.getNextPrefix());
	}

	public boolean isSingleValue() {
		return singleValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((end == null) ? 0 : end.hashCode());
		result = (prime * result) + (singleValue ? 1231 : 1237);
		result = (prime * result) + ((start == null) ? 0 : start.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final ByteArrayRange other = (ByteArrayRange) obj;
		if (end == null) {
			if (other.end != null) {
				return false;
			}
		}
		else if (!end.equals(other.end)) {
			return false;
		}
		if (singleValue != other.singleValue) {
			return false;
		}
		if (start == null) {
			if (other.start != null) {
				return false;
			}
		}
		else if (!start.equals(other.start)) {
			return false;
		}
		return true;
	}

	public boolean intersects(
			final ByteArrayRange other ) {
		return (((getStart().compareTo(other.getEnd())) <= 0) && ((getEnd().compareTo(other.getStart())) >= 0));
	}

	public ByteArrayRange intersection(
			final ByteArrayRange other ) {
		return new ByteArrayRange(
				start.compareTo(other.start) <= 0 ? other.start : start,
				end.compareTo(other.end) >= 0 ? other.end : end);
	}

	public ByteArrayRange union(
			final ByteArrayRange other ) {
		return new ByteArrayRange(
				start.compareTo(other.start) <= 0 ? start : other.start,
				end.compareTo(other.end) >= 0 ? end : other.end);
	}

	@Override
	public int compareTo(
			final ByteArrayRange other ) {
		final int diff = getStart().compareTo(
				other.getStart());
		return diff != 0 ? diff : getEnd().compareTo(
				other.getEnd());
	}

	public static enum MergeOperation {
		UNION,
		INTERSECTION
	}

	public static final Collection<ByteArrayRange> mergeIntersections(
			final Collection<ByteArrayRange> ranges,
			final MergeOperation op ) {
		List<ByteArrayRange> rangeList = new ArrayList<>(
				ranges);
		// sort order so the first range can consume following ranges
		Collections.<ByteArrayRange> sort(rangeList);
		final List<ByteArrayRange> result = new ArrayList<ByteArrayRange>();
		for (int i = 0; i < rangeList.size();) {
			ByteArrayRange r1 = rangeList.get(i);
			int j = i + 1;
			for (; j < rangeList.size(); j++) {
				final ByteArrayRange r2 = rangeList.get(j);
				if (r1.intersects(r2)) {
					if (op.equals(MergeOperation.UNION)) {
						r1 = r1.union(r2);
					}
					else {
						r1 = r1.intersection(r2);
					}
				}
				else {
					break;
				}
			}
			i = j;
			result.add(r1);
		}
		return result;
	}
}
