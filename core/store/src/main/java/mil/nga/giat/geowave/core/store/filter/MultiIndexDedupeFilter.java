package mil.nga.giat.geowave.core.store.filter;

/**
 * This filter will perform de-duplication using the combination of data adapter
 * ID and data ID to determine uniqueness. This should only be used client-side
 * because multiple indices would use a server-side scan per index anyways but
 * can be used distributed if necessary. This will cache every ID that streams
 * in regardless of whether its duplicated within a given index, so that it will
 * support deduplication across multiple indices.
 * 
 */
public class MultiIndexDedupeFilter extends
		DedupeFilter
{
	private boolean multiIndexSupport = true;

	public MultiIndexDedupeFilter() {
		super();
	}

	@Override
	protected boolean supportsMultipleIndices() {
		return multiIndexSupport;
	}

	public void setMultiIndexSupportEnabled(
			final boolean multiIndexSupport ) {
		this.multiIndexSupport = multiIndexSupport;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {
			(byte) (multiIndexSupport ? 0 : 1)
		};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if (bytes.length > 0) {
			multiIndexSupport = (bytes[0] == 0);
		}
	}

}
