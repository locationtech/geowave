/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.analytic.partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;

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

		private ByteArrayId id;
		private ByteArrayId groupId = null;
		private boolean isPrimary;

		public ByteArrayId getId() {
			return id;
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
				final ByteArrayId id,
				final boolean primary ) {
			super();
			this.id = id;
			this.isPrimary = primary;
		}

		@Override
		public String toString() {
			return "PartitionData [id=" + Hex.encodeHexString(id.getBytes()) + ", groupId="
					+ (groupId == null ? "null" : groupId.getString()) + ", isPrimary=" + isPrimary + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((id == null) ? 0 : id.hashCode());
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
			if (id == null) {
				if (other.id != null) {
					return false;
				}
			}
			else if (!id.equals(other.id)) {
				return false;
			}
			return true;
		}

		@Override
		public void readFields(
				final DataInput dInput )
				throws IOException {
			final int idSize = dInput.readInt();
			final byte[] idBytes = new byte[idSize];
			dInput.readFully(idBytes);
			id = new ByteArrayId(
					idBytes);

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
			final byte[] outputId = id.getBytes();
			dOutput.writeInt(outputId.length);
			dOutput.write(outputId);

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
				boolean isPrimary ) {
			this.isPrimary = isPrimary;
		}
	}

}
