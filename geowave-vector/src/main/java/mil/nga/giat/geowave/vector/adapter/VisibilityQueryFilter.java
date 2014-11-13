package mil.nga.giat.geowave.vector.adapter;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.store.data.PersistentValue;
import mil.nga.giat.geowave.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.store.index.CommonIndexValue;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.ColumnVisibility.Node;

/**
 * Return a row with a specific visibility (e.g. the transaction id)
 * 
 */
public class VisibilityQueryFilter implements
		DistributableQueryFilter
{
	byte[] authorization;

	protected VisibilityQueryFilter() {}

	@Override
	public boolean accept(
			IndexedPersistenceEncoding persistenceEncoding ) {

		for (PersistentValue<? extends CommonIndexValue> value : persistenceEncoding.getCommonData().getValues()) {
			ColumnVisibility v = new ColumnVisibility(
					value.getValue().getVisibility());
			if (matches(
					v.getParseTree(),
					value.getValue().getVisibility())) return true;
		}
		return false;
	}

	private boolean matches(
			Node vTree,
			byte[] expression ) {
		if (vTree.getTerm(
				expression).equals(
				authorization)) return true;
		boolean result = false;
		for (Node child : vTree.getChildren()) {
			result |= matches(
					child,
					expression);
		}
		return result;
	}

	@Override
	public byte[] toBinary() {
		return authorization;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		buf.put(bytes);
		authorization = buf.array();
	}

}
