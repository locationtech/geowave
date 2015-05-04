package mil.nga.giat.geowave.core.store.adapter;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;

import org.junit.Test;

public class AbstractDataAdapterTest
{

	public static void main(
			final String[] args ) {
		new AbstractDataAdapterTest().testAbstractDataAdapterPersistance();
	}

	// Mock class instantiating abstract class so we can test logic
	// contained in abstract class.
	protected static class MockAbstractDataAdapter extends
			AbstractDataAdapter<Integer>
	{
		@SuppressWarnings("unused")
		public MockAbstractDataAdapter() {
			super();
		}

		@SuppressWarnings("unused")
		public MockAbstractDataAdapter(
				final List<PersistentIndexFieldHandler<Integer, ? extends CommonIndexValue, Object>> indexFieldHandlers,
				final List<NativeFieldHandler<Integer, Object>> nativeFieldHandlers,
				final Object defaultTypeData ) {
			super(
					indexFieldHandlers,
					nativeFieldHandlers,
					defaultTypeData);
		}

		private static final ByteArrayId INTEGER = new ByteArrayId(
				"TestInteger");
		private static final ByteArrayId ID = new ByteArrayId(
				"TestIntegerAdapter");

		public MockAbstractDataAdapter(
				final List<PersistentIndexFieldHandler<Integer, // RowType
				? extends CommonIndexValue, // IndexFieldType
				Object // NativeFieldType
				>> _indexFieldHandlers,
				final List<NativeFieldHandler<Integer, // RowType
				Object // FieldType
				>> _nativeFieldHandlers ) {
			super(
					_indexFieldHandlers,
					_nativeFieldHandlers);
		}

		@Override
		public ByteArrayId getAdapterId() {
			return MockAbstractDataAdapter.ID;
		}

		/**
		 * 
		 * Return the adapter ID
		 * 
		 * @return a unique identifier for this adapter
		 */
		@Override
		public boolean isSupported(
				final Integer entry ) {
			return true;
		}

		@Override
		public ByteArrayId getDataId(
				final Integer entry ) {
			return new ByteArrayId(
					"DataID" + entry.toString());
		}

		@SuppressWarnings({
			"unchecked",
			"rawtypes"
		})
		@Override
		public FieldReader getReader(
				final ByteArrayId fieldId ) {
			if (fieldId.equals(INTEGER)) {
				return FieldUtils.getDefaultReaderForClass(Integer.class);
			}
			else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultReaderForClass(String.class);
			}
			return null;
		}

		@SuppressWarnings({
			"unchecked",
			"rawtypes"
		})
		@Override
		public FieldWriter getWriter(
				final ByteArrayId fieldId ) {
			if (fieldId.equals(INTEGER)) {
				return FieldUtils.getDefaultWriterForClass(Integer.class);
			}
			else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultWriterForClass(String.class);
			}
			return null;
		}

		@Override
		protected RowBuilder<Integer, Object> newBuilder() {
			return new RowBuilder<Integer, Object>() {
				@SuppressWarnings("unused")
				private String id;
				private Integer intValue;

				@Override
				public void setField(
						final PersistentValue<Object> fieldValue ) {
					if (fieldValue.getId().equals(
							INTEGER)) {
						intValue = (Integer) fieldValue.getValue();
					}
					else if (fieldValue.getId().equals(
							ID)) {
						id = (String) fieldValue.getValue();
					}
				}

				@Override
				public Integer buildRow(
						final ByteArrayId dataId ) {
					return new Integer(
							intValue);
				}
			};
		}

	} // class MockAbstractDataAdapter

	// *************************************************************************
	//
	// Test index field type for dimension.
	//
	// *************************************************************************
	protected static class TestIndexFieldType implements
			CommonIndexValue
	{
		private final Integer indexValue;

		public TestIndexFieldType(
				final Integer _indexValue ) {
			indexValue = _indexValue;
		}

		@Override
		public byte[] getVisibility() {
			return null;
		}

		@Override
		public void setVisibility(
				final byte[] visibility ) {}

		@Override
		public boolean overlaps(
				final DimensionField[] dimensions,
				final NumericData[] rangeData ) {
			return true;
		}

	}

	// *************************************************************************
	//
	// Test class that implements PersistentIndexFieldHandler<T> for
	// instantiation of MockAbstractDataAdapter object.
	//
	// *************************************************************************
	protected static class TestPersistentIndexFieldHandler implements
			PersistentIndexFieldHandler<Integer, TestIndexFieldType, Object>
	{

		public TestPersistentIndexFieldHandler() {}

		@Override
		public ByteArrayId[] getNativeFieldIds() {
			return new ByteArrayId[] {
				MockAbstractDataAdapter.INTEGER
			};
		}

		// toIndexValue simply increments each digit in number.
		@Override
		public TestIndexFieldType toIndexValue(
				final Integer row ) {

			final String sRow = row.toString();
			final int numDigits = sRow.length();
			String sNewRow = new String();
			final char[] newDigit = new char[1];
			for (int i = 0; i < numDigits; i++) {
				final char digit = sRow.charAt(i);
				switch (digit) {
					case '0':
						newDigit[0] = '1';
						break;
					case '1':
						newDigit[0] = '2';
						break;
					case '2':
						newDigit[0] = '3';
						break;
					case '3':
						newDigit[0] = '4';
						break;
					case '4':
						newDigit[0] = '5';
						break;
					case '5':
						newDigit[0] = '6';
						break;
					case '6':
						newDigit[0] = '7';
						break;
					case '7':
						newDigit[0] = '8';
						break;
					case '8':
						newDigit[0] = '9';
						break;
					case '9':
						newDigit[0] = '0';
						break;
				}
				sNewRow = sNewRow.concat(new String(
						newDigit));
			}
			return new TestIndexFieldType(
					Integer.decode(sNewRow));
		}

		// toNativeValues decrements each digit in the value.
		@SuppressWarnings("unchecked")
		@Override
		public PersistentValue<Object>[] toNativeValues(
				final TestIndexFieldType _indexValue ) {

			final String sRow = _indexValue.indexValue.toString();
			final int numDigits = sRow.length();
			String sNewRow = new String();
			final char[] newDigit = new char[1];
			for (int i = 0; i < numDigits; i++) {
				final char digit = sRow.charAt(i);
				switch (digit) {
					case '0':
						newDigit[0] = '9';
						break;
					case '1':
						newDigit[0] = '0';
						break;
					case '2':
						newDigit[0] = '1';
						break;
					case '3':
						newDigit[0] = '2';
						break;
					case '4':
						newDigit[0] = '3';
						break;
					case '5':
						newDigit[0] = '4';
						break;
					case '6':
						newDigit[0] = '5';
						break;
					case '7':
						newDigit[0] = '6';
						break;
					case '8':
						newDigit[0] = '7';
						break;
					case '9':
						newDigit[0] = '8';
						break;
				}
				sNewRow = sNewRow.concat(new String(
						newDigit));
			}
			final Integer newValue = Integer.decode(sNewRow);

			return new PersistentValue[] {
				new PersistentValue<Object>(
						MockAbstractDataAdapter.INTEGER,
						newValue)
			};
		}

		@Override
		public byte[] toBinary() {
			return new byte[0];
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}

	}

	// *************************************************************************
	//
	// Test class that implements NativeFieldHandler<RowType,FieldType>
	// for instantiation of MockAbstractDataAdapter object.
	//
	// *************************************************************************
	protected static class TestNativeFieldHandler implements
			NativeFieldHandler<Integer, Object>
	{

		@Override
		public ByteArrayId getFieldId() {
			return MockAbstractDataAdapter.INTEGER;
		}

		@Override
		public Object getFieldValue(
				final Integer row ) {
			return row;
		}

	}

	// *************************************************************************
	//
	// Test implementation on interface DimensionField for use by
	// TestIndexModel.
	//
	// *************************************************************************
	protected class TestDimensionField implements
			DimensionField<TestIndexFieldType>
	{
		final ByteArrayId fieldId;

		public TestDimensionField() {
			fieldId = new ByteArrayId(
					new String(
							"TestDimensionField1"));
		}

		@Override
		public double normalize(
				final double value ) {
			return 0;
		}

		@Override
		public BinRange[] getNormalizedRanges(
				final NumericData range ) {
			return null;
		}

		@Override
		public byte[] toBinary() {
			return null;
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}

		@Override
		public NumericData getNumericData(
				final TestIndexFieldType dataElement ) {
			return null;
		}

		@Override
		public ByteArrayId getFieldId() {
			return fieldId;
		}

		@Override
		public FieldWriter<?, TestIndexFieldType> getWriter() {
			return null;
		}

		@Override
		public FieldReader<TestIndexFieldType> getReader() {
			return null;
		}

		@Override
		public NumericDimensionDefinition getBaseDefinition() {
			return null;
		}

		@Override
		public double getRange() {
			return 0;
		}

		@Override
		public double denormalize(
				final double value ) {
			return 0;
		}

		@Override
		public NumericRange getDenormalizedRange(
				final BinRange range ) {
			return null;
		}

		@Override
		public int getFixedBinIdSize() {
			return 0;
		}

		@Override
		public NumericRange getBounds() {
			return null;
		}

		@Override
		public NumericData getFullRange() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean isCompatibleDefinition(
				NumericDimensionDefinition otherDimensionDefinition ) {
			return false;
		}

	}

	// *************************************************************************
	//
	// Test index model class for use in testing encoding by
	// AbstractDataAdapter.
	//
	// *************************************************************************
	protected class TestIndexModel implements
			CommonIndexModel
	{

		private final TestDimensionField[] dimensionFields;

		public TestIndexModel() {
			dimensionFields = new TestDimensionField[3];
			dimensionFields[0] = new TestDimensionField();
			dimensionFields[1] = new TestDimensionField();
			dimensionFields[2] = new TestDimensionField();
		}

		@Override
		public FieldReader<CommonIndexValue> getReader(
				final ByteArrayId fieldId ) {
			return null;
		}

		@Override
		public FieldWriter<Object, CommonIndexValue> getWriter(
				final ByteArrayId fieldId ) {
			return null;
		}

		@Override
		public byte[] toBinary() {
			return null;
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}

		@Override
		public TestDimensionField[] getDimensions() {
			return dimensionFields;
		}

		@Override
		public String getId() {
			return null;
		}

	}

	@Test
	// *************************************************************************
	//
	// Test encode(..) and decode(..) methods of AbstractDataAdapter via
	// instantiation of MockAbstractDataAdapterTest.
	//
	// *************************************************************************
	public void testAbstractDataAdapterEncodeDecode() {
		// To instantiate MockAbstractDataAdapter, need to create
		// array of indexFieldHandlers and array of nativeFieldHandlers.
		final ArrayList<PersistentIndexFieldHandler<Integer, ? extends CommonIndexValue, Object>> indexFieldHandlers = new ArrayList<PersistentIndexFieldHandler<Integer, ? extends CommonIndexValue, Object>>();
		indexFieldHandlers.add(new TestPersistentIndexFieldHandler());

		final ArrayList<NativeFieldHandler<Integer, Object>> nativeFieldHandlers = new ArrayList<NativeFieldHandler<Integer, Object>>();
		nativeFieldHandlers.add(new TestNativeFieldHandler());

		final MockAbstractDataAdapter mockAbstractDataAdapter = new MockAbstractDataAdapter(
				indexFieldHandlers,
				nativeFieldHandlers);
		final TestIndexModel testIndexModel = new TestIndexModel();
		final Integer beforeValue = 123456;
		final AdapterPersistenceEncoding testEncoding = mockAbstractDataAdapter.encode(
				beforeValue,
				testIndexModel);
		final Integer afterValue = mockAbstractDataAdapter.decode(
				new IndexedAdapterPersistenceEncoding(
						testEncoding.getAdapterId(),
						testEncoding.getDataId(),
						null,
						1,
						testEncoding.getCommonData(),
						testEncoding.getAdapterExtendedData()),
				new Index(
						null,
						testIndexModel));

		Assert.assertEquals(
				"EncodeDecode_test",
				beforeValue,
				afterValue);
	}

	@Test
	public void testAbstractDataAdapterPersistance() {
		final ArrayList<PersistentIndexFieldHandler<Integer, ? extends CommonIndexValue, Object>> indexFieldHandlers = new ArrayList<PersistentIndexFieldHandler<Integer, ? extends CommonIndexValue, Object>>();
		indexFieldHandlers.add(new TestPersistentIndexFieldHandler());

		final ArrayList<NativeFieldHandler<Integer, Object>> nativeFieldHandlers = new ArrayList<NativeFieldHandler<Integer, Object>>();
		nativeFieldHandlers.add(new TestNativeFieldHandler());

		final MockAbstractDataAdapter mockAbstractDataAdapter = new MockAbstractDataAdapter(
				indexFieldHandlers,
				nativeFieldHandlers);

		final MockAbstractDataAdapter obj = PersistenceUtils.fromBinary(
				PersistenceUtils.toBinary(mockAbstractDataAdapter),
				MockAbstractDataAdapter.class);

		// TODO is there another test?
		Assert.assertNotNull(obj);
	}
}
