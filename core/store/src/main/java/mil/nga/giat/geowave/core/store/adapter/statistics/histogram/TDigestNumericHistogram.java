package mil.nga.giat.geowave.core.store.adapter.statistics.histogram;

import java.nio.ByteBuffer;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;

public class TDigestNumericHistogram implements
		NumericHistogram
{
	private static final double DEFAULT_COMPRESSION = 100;
	private TDigest tdigest;
	private long count = 0;

	public TDigestNumericHistogram() {
		super();
		tdigest = TDigest.createMergingDigest(DEFAULT_COMPRESSION);
	}

	@Override
	public void merge(
			final NumericHistogram other ) {
		if (other instanceof TDigestNumericHistogram) {
			tdigest.add(((TDigestNumericHistogram) other).tdigest);
			count += ((TDigestNumericHistogram) other).count;
		}
	}

	@Override
	public void add(
			final double v ) {
		tdigest.add(v);
		count++;
	}

	@Override
	public double quantile(
			final double q ) {
		return tdigest.quantile(q);
	}

	@Override
	public double cdf(
			final double val ) {
		return tdigest.cdf(val);
	}

	@Override
	public int bufferSize() {
		return tdigest.smallByteSize() + ByteArrayUtils.variableLengthEncode(count).length;
	}

	@Override
	public void toBinary(
			final ByteBuffer buffer ) {
		tdigest.asSmallBytes(buffer);
		final byte[] remaining = new byte[buffer.remaining()];
		buffer.get(remaining);
		count = ByteArrayUtils.variableLengthDecode(remaining);
	}

	@Override
	public void fromBinary(
			final ByteBuffer buffer ) {
		tdigest = MergingDigest.fromBytes(buffer);
		buffer.put(ByteArrayUtils.variableLengthEncode(count));
	}

	@Override
	public double getMaxValue() {
		return tdigest.getMax();
	}

	@Override
	public double getMinValue() {
		return tdigest.getMin();
	}

	@Override
	public long getTotalCount() {
		return count;
	}

	@Override
	public double sum(
			final double val,
			final boolean inclusive ) {
		final double sum = tdigest.cdf(val) * count;
		if (inclusive && (sum < 1)) {
			return 1.0;
		}
		return sum;
	}

}
