package mil.nga.giat.geowave.analytic.partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;

/**
 * Provide a partition for a data item.
 *
 *
 * Multiple partitions are permitted. Only one partition is consider primary. A
 * primary partition is the partition for an item in which the item is processed
 * on behalf of itself. All other partitions are those partitions that require
 * visibility to the a specific item for other items to reference. This approach
 * supports nearest neighbor type queries. Consider that an item can only
 * discover neighbors in its partition. However, the item can be discovered as a
 * nearest neighbor in those partitions in which the item participates as a none
 * primary.
 *
 * @param <T>
 */
public interface Partitioner<T> extends
		Serializable
{

	public void initialize(
			final JobContext context,
			final Class<?> scope )
			throws IOException;

	public List<PartitionData> getCubeIdentifiers(
			final T entry );

	public void partition(
			T entry,
			PartitionDataCallback callback )
			throws Exception;

	public Collection<ParameterEnum<?>> getParameters();

	public void setup(
			PropertyManagement runTimeProperties,
			Class<?> scope,
			Configuration configuration );

	public static interface PartitionDataCallback
	{
		void partitionWith(
				PartitionData data )
				throws Exception;
	}

	/**
	 * Represents a partition associated with a specific item. The partition is
	 * marked as primary or secondary. A secondary partition is a neighboring
	 * partition to an item. The intent is inspect neighbor partitions to handle
	 * edge cases.
	 *
	 */
	public static class PartitionData implements
			Serializable,
			Writable
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 1L;

		private ByteArrayId partitionKey;
		private ByteArrayId sortKey;
		private ByteArrayId groupId = null;
		private boolean isPrimary;

		public ByteArrayId getPartitionKey() {
			return partitionKey;
		}

		public ByteArrayId getSortKey() {
			return sortKey;
		}

		public ByteArrayId getCompositeKey() {
			return new ByteArrayId(
					ByteArrayUtils.combineArrays(
							partitionKey.getBytes(),
							sortKey.getBytes()));
		}

		public ByteArrayId getGroupId() {
			return groupId;
		}

		public void setGroupId(
				final ByteArrayId groupId ) {
			this.groupId = groupId;
		}

		public boolean isPrimary() {
			return isPrimary;
		}

		public PartitionData() {}

		public PartitionData(
				final ByteArrayId partitionKey,
				final ByteArrayId sortKey,
				final boolean primary ) {
			super();
			this.partitionKey = partitionKey;
			this.sortKey = sortKey;
			this.isPrimary = primary;
		}

		@Override
		public String toString() {
			return "PartitionData [partitionKey=" + Hex.encodeHexString(partitionKey.getBytes()) + ", sortKey="
					+ Hex.encodeHexString(sortKey.getBytes()) + ", groupId="
					+ (groupId == null ? "null" : groupId.getString()) + ", isPrimary=" + isPrimary + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((partitionKey == null) ? 0 : partitionKey.hashCode());
			result = (prime * result) + ((sortKey == null) ? 0 : sortKey.hashCode());
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
			final PartitionData other = (PartitionData) obj;
			if (partitionKey == null) {
				if (other.partitionKey != null) {
					return false;
				}
			}
			else if (!partitionKey.equals(other.partitionKey)) {
				return false;
			}
			if (sortKey == null) {
				if (other.sortKey != null) {
					return false;
				}
			}
			else if (!sortKey.equals(other.sortKey)) {
				return false;
			}
			return true;
		}

		@Override
		public void readFields(
				final DataInput dInput )
				throws IOException {
			final int partitionKeySize = dInput.readInt();
			final byte[] partitionKeyBytes = new byte[partitionKeySize];
			dInput.readFully(partitionKeyBytes);
			partitionKey = new ByteArrayId(
					partitionKeyBytes);
			final int sortKeySize = dInput.readInt();
			final byte[] sortKeyBytes = new byte[sortKeySize];
			dInput.readFully(sortKeyBytes);
			sortKey = new ByteArrayId(
					sortKeyBytes);

			final int groupIdSize = dInput.readInt();
			if (groupIdSize > 0) {
				final byte[] groupIdIdBytes = new byte[groupIdSize];
				dInput.readFully(groupIdIdBytes);
				groupId = new ByteArrayId(
						groupIdIdBytes);
			}

			isPrimary = dInput.readBoolean();
		}

		@Override
		public void write(
				final DataOutput dOutput )
				throws IOException {
			final byte[] outputPartitionKey = partitionKey.getBytes();
			dOutput.writeInt(outputPartitionKey.length);
			dOutput.write(outputPartitionKey);
			final byte[] outputSortKey = sortKey.getBytes();
			dOutput.writeInt(outputSortKey.length);
			dOutput.write(outputSortKey);
			if (groupId != null) {
				final byte[] groupOutputId = groupId.getBytes();
				dOutput.writeInt(groupOutputId.length);
				dOutput.write(groupOutputId);
			}
			else {
				dOutput.writeInt(0);
			}

			dOutput.writeBoolean(isPrimary);

		}

		public void setPrimary(
				final boolean isPrimary ) {
			this.isPrimary = isPrimary;
		}
	}

}
