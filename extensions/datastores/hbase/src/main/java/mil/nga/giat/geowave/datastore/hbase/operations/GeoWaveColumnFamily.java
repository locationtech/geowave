package mil.nga.giat.geowave.datastore.hbase.operations;

import java.util.Arrays;

import org.apache.hadoop.hbase.HColumnDescriptor;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.StringUtils;

public interface GeoWaveColumnFamily
{
	public HColumnDescriptor toColumnDescriptor();

	public static interface GeoWaveColumnFamilyFactory
	{
		public GeoWaveColumnFamily fromColumnDescriptor(
				HColumnDescriptor column );
	}

	public static class StringColumnFamily implements
			GeoWaveColumnFamily
	{
		private String columnFamily;

		public StringColumnFamily(
				String columnFamily ) {
			this.columnFamily = columnFamily;
		}

		@Override
		public HColumnDescriptor toColumnDescriptor() {
			return new HColumnDescriptor(
					columnFamily);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((columnFamily == null) ? 0 : columnFamily.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				Object obj ) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			StringColumnFamily other = (StringColumnFamily) obj;
			if (columnFamily == null) {
				if (other.columnFamily != null) return false;
			}
			else if (!columnFamily.equals(other.columnFamily)) return false;
			return true;
		}

	}

	public static class StringColumnFamilyFactory implements
			GeoWaveColumnFamilyFactory
	{
		public static StringColumnFamilyFactory getSingletonInstance() {
			return SINGLETON_INSTANCE;
		}

		private static final StringColumnFamilyFactory SINGLETON_INSTANCE = new StringColumnFamilyFactory();

		private StringColumnFamilyFactory() {}

		@Override
		public GeoWaveColumnFamily fromColumnDescriptor(
				HColumnDescriptor column ) {

			return new StringColumnFamily(
					column.getNameAsString());
		}

	}

	public static class ByteArrayColumnFamily implements
			GeoWaveColumnFamily
	{
		private byte[] columnFamily;

		public ByteArrayColumnFamily(
				byte[] columnFamily ) {
			this.columnFamily = columnFamily;
		}

		@Override
		public HColumnDescriptor toColumnDescriptor() {
			return new HColumnDescriptor(
					columnFamily);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + Arrays.hashCode(columnFamily);
			return result;
		}

		@Override
		public boolean equals(
				Object obj ) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			ByteArrayColumnFamily other = (ByteArrayColumnFamily) obj;
			if (!Arrays.equals(
					columnFamily,
					other.columnFamily)) return false;
			return true;
		}

	}

	public static class ByteArrayColumnFamilyFactory implements
			GeoWaveColumnFamilyFactory
	{
		public static ByteArrayColumnFamilyFactory getSingletonInstance() {
			return SINGLETON_INSTANCE;
		}

		private static final ByteArrayColumnFamilyFactory SINGLETON_INSTANCE = new ByteArrayColumnFamilyFactory();

		private ByteArrayColumnFamilyFactory() {}

		@Override
		public GeoWaveColumnFamily fromColumnDescriptor(
				HColumnDescriptor column ) {

			return new ByteArrayColumnFamily(
					column.getName());
		}

	}
}
