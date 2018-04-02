package mil.nga.giat.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import mil.nga.giat.geowave.core.index.persist.Persistable;

public class InsertionIds implements
		Persistable
{
	private Collection<SinglePartitionInsertionIds> partitionKeys;
	private List<ByteArrayId> compositeInsertionIds;
	private int size = -1;

	public InsertionIds() {
		partitionKeys = new ArrayList<SinglePartitionInsertionIds>();
	}

	public InsertionIds(
			final List<ByteArrayId> sortKeys ) {
		this(
				new SinglePartitionInsertionIds(
						null,
						sortKeys));
	}

	public InsertionIds(
			final ByteArrayId partitionKey ) {
		this(
				new SinglePartitionInsertionIds(
						partitionKey));
	}

	public InsertionIds(
			final ByteArrayId partitionKey,
			final List<ByteArrayId> sortKeys ) {
		this(
				new SinglePartitionInsertionIds(
						partitionKey,
						sortKeys));
	}

	public InsertionIds(
			final SinglePartitionInsertionIds singePartitionKey ) {
		this(
				Arrays.asList(singePartitionKey));
	}

	public InsertionIds(
			final Collection<SinglePartitionInsertionIds> partitionKeys ) {
		this.partitionKeys = partitionKeys;
	}

	public Collection<SinglePartitionInsertionIds> getPartitionKeys() {
		return partitionKeys;
	}

	public boolean isEmpty() {
		if (compositeInsertionIds != null) {
			return compositeInsertionIds.isEmpty();
		}
		if ((partitionKeys == null) || partitionKeys.isEmpty()) {
			return true;
		}
		return false;
	}

	public boolean hasDuplicates() {
		if (compositeInsertionIds != null) {
			return compositeInsertionIds.size() >= 2;
		}
		if ((partitionKeys == null) || partitionKeys.isEmpty()) {
			return false;
		}
		if (partitionKeys.size() > 1) {
			return true;
		}
		final SinglePartitionInsertionIds partition = partitionKeys.iterator().next();
		if ((partition.getSortKeys() == null) || (partition.getSortKeys().size() <= 1)) {
			return false;
		}
		return true;
	}

	public int getSize() {
		if (size >= 0) {
			return size;
		}
		if (compositeInsertionIds != null) {
			size = compositeInsertionIds.size();
			return size;
		}
		if ((partitionKeys == null) || partitionKeys.isEmpty()) {
			size = 0;
			return size;
		}
		int internalSize = 0;
		for (final SinglePartitionInsertionIds k : partitionKeys) {
			final List<ByteArrayId> i = k.getCompositeInsertionIds();
			if ((i != null) && !i.isEmpty()) {
				internalSize += i.size();
			}
		}
		size = internalSize;
		return size;
	}

	public QueryRanges asQueryRanges() {
		return new QueryRanges(
				Collections2.transform(
						partitionKeys,
						new Function<SinglePartitionInsertionIds, SinglePartitionQueryRanges>() {
							@Override
							public SinglePartitionQueryRanges apply(
									final SinglePartitionInsertionIds input ) {
								return new SinglePartitionQueryRanges(
										input.getPartitionKey(),
										Collections2.transform(
												input.getSortKeys(),
												new Function<ByteArrayId, ByteArrayRange>() {
													@Override
													public ByteArrayRange apply(
															final ByteArrayId input ) {
														return new ByteArrayRange(
																input,
																input,
																false);
													}
												}));
							}
						}));
	}

	public List<ByteArrayId> getCompositeInsertionIds() {
		if (compositeInsertionIds != null) {
			return compositeInsertionIds;
		}
		if ((partitionKeys == null) || partitionKeys.isEmpty()) {
			return Collections.EMPTY_LIST;
		}
		final List<ByteArrayId> internalCompositeInsertionIds = new ArrayList<>();
		for (final SinglePartitionInsertionIds k : partitionKeys) {
			final List<ByteArrayId> i = k.getCompositeInsertionIds();
			if ((i != null) && !i.isEmpty()) {
				internalCompositeInsertionIds.addAll(i);
			}
		}
		compositeInsertionIds = internalCompositeInsertionIds;
		return compositeInsertionIds;
	}

	public boolean contains(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		for (final SinglePartitionInsertionIds p : partitionKeys) {
			if (((partitionKey == null) && (p.getPartitionKey() == null))
					|| ((partitionKey != null) && partitionKey.equals(p.getPartitionKey()))) {
				// partition key matches find out if sort key is contained
				if (sortKey == null) {
					return true;
				}
				if ((p.getSortKeys() != null) && p.getSortKeys().contains(
						sortKey)) {
					return true;
				}
				return false;
			}
		}
		return false;
	}

	public Pair<ByteArrayId, ByteArrayId> getFirstPartitionAndSortKeyPair() {
		if (partitionKeys == null) {
			return null;
		}
		for (final SinglePartitionInsertionIds p : partitionKeys) {
			if ((p.getSortKeys() != null) && !p.getSortKeys().isEmpty()) {
				return new ImmutablePair<ByteArrayId, ByteArrayId>(
						p.getPartitionKey(),
						p.getSortKeys().get(
								0));
			}
			else if ((p.getPartitionKey() != null)) {
				return new ImmutablePair<ByteArrayId, ByteArrayId>(
						p.getPartitionKey(),
						null);
			}
		}
		return null;
	}

	@Override
	public byte[] toBinary() {
		if ((partitionKeys != null) && !partitionKeys.isEmpty()) {
			final List<byte[]> partitionKeysBinary = new ArrayList<>(
					partitionKeys.size());
			int totalSize = 4;
			for (final SinglePartitionInsertionIds id : partitionKeys) {
				final byte[] binary = id.toBinary();
				totalSize += (4 + binary.length);
				partitionKeysBinary.add(binary);
			}
			final ByteBuffer buf = ByteBuffer.allocate(totalSize);
			buf.putInt(totalSize);
			for (final byte[] binary : partitionKeysBinary) {
				buf.putInt(binary.length);
				buf.put(binary);
			}
			return buf.array();
		}
		else {
			return ByteBuffer.allocate(
					4).putInt(
					0).array();
		}
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int size = buf.getInt();
		if (size > 0) {
			partitionKeys = new ArrayList<>(
					size);
			for (int i = 0; i < size; i++) {
				final int length = buf.getInt();
				final byte[] pBytes = new byte[length];
				buf.get(pBytes);
				final SinglePartitionInsertionIds pId = new SinglePartitionInsertionIds();
				pId.fromBinary(pBytes);
				partitionKeys.add(pId);
			}
		}
		else {
			partitionKeys = new ArrayList<>();
		}
	}
}
