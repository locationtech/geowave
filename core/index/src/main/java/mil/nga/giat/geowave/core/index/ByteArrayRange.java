package mil.nga.giat.geowave.core.index;

import java.util.Collections;
import java.util.List;

/***
 * Defines a unit interval on a number line
 * 
 */
public class ByteArrayRange implements
		Comparable<ByteArrayRange>
{
	private final ByteArrayId start;
	private final ByteArrayId end;
	private boolean singleValue;

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
			boolean singleValue ) {
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

	public boolean isSingleValue() {
		return singleValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((end == null) ? 0 : end.hashCode());
		result = prime * result + (singleValue ? 1231 : 1237);
		result = prime * result + ((start == null) ? 0 : start.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		ByteArrayRange other = (ByteArrayRange) obj;
		if (end == null) {
			if (other.end != null) return false;
		}
		else if (!end.equals(other.end)) return false;
		if (singleValue != other.singleValue) return false;
		if (start == null) {
			if (other.start != null) return false;
		}
		else if (!start.equals(other.start)) return false;
		return true;
	}

	public boolean intersects(
			ByteArrayRange other ) {
		return ((getStart().compareTo(other.getEnd())) <= 0 && (getEnd().compareTo(other.getStart())) >= 0);
	}

	public ByteArrayRange merge(
			ByteArrayRange other ) {
		return new ByteArrayRange(
				this.start.compareTo(other.start) <= 0 ? this.start : other.start,
				this.end.compareTo(other.end) >= 0 ? this.end : other.end);
	}

	@Override
	public int compareTo(
			ByteArrayRange other ) {
		final int diff = getStart().compareTo(
				other.getStart());
		return diff != 0 ? diff : getEnd().compareTo(
				other.getEnd());
	}

	public static final void mergeIntersections(
			List<ByteArrayRange> ranges,
			int maxRanges ) {
		// sort order so the first range can consume following ranges
		Collections.<ByteArrayRange> sort(ranges);
		// merge in place
		for (int i = 0; i < ranges.size(); i++) {
			ByteArrayRange r1 = ranges.get(i);
			for (int j = i + 1; j < ranges.size(); j++) {
				final ByteArrayRange r2 = ranges.get(j);
				if (r1.intersects(r2)) {
					r1 = r1.merge(r2);
					ranges.remove(j);
					j--;
					ranges.set(
							i,
							r1);
				}
			}
		}
	}

}
