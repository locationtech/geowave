package mil.nga.giat.geowave.store.adapter;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.store.data.PersistentValue;
import mil.nga.giat.geowave.store.data.field.BasicReader.IntReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.StringReader;
import mil.nga.giat.geowave.store.data.field.BasicWriter.IntWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.StringWriter;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldWriter;
import mil.nga.giat.geowave.store.dimension.DimensionField;
import mil.nga.giat.geowave.store.index.CommonIndexModel;
import mil.nga.giat.geowave.store.index.CommonIndexValue;

import org.junit.Test;

public class AbstractDataAdapterTest
{

	public static void main(
			String[] args ) {
		new AbstractDataAdapterTest().testAbstractDataAdapterPersistance();
	}

	// Mock class instantiating abstract class so we can test logic
	// contained in abstract class.
	private static class MockAbstractDataAdapter extends AbstractDataAdapter<Integer>
	{
		@SuppressWarnings("unused")
		public MockAbstractDataAdapter() {
			super();
		}

		@SuppressWarnings("unused")
		public MockAbstractDataAdapter(
				List<PersistentIndexFieldHandler<Integer, ? extends CommonIndexValue, Object>> indexFieldHandlers,
				List<NativeFieldHandler<Integer, Object>> nativeFieldHandlers,
				Object defaultTypeData ) {
			super(
					indexFieldHandlers,
					nativeFieldHandlers,
					defaultTypeData);
		}

		private static final ByteArrayId INTEGER = new ByteArrayId("TestInteger");
		private static final ByteArrayId ID = new ByteArrayId("TestIntegerAdapter");
		
		public MockAbstractDataAdapter(
				final List<
						PersistentIndexFieldHandler<
								Integer, 							// RowType
								? extends CommonIndexValue,		// IndexFieldType
								Object								// NativeFieldType
						>
				> _indexFieldHandlers,
				final List<
						NativeFieldHandler<
								Integer, 							// RowType
								Object								// FieldType
						>
				> _nativeFieldHandlers ) {
			super( _indexFieldHandlers, _nativeFieldHandlers );
		}

		@Override
		public ByteArrayId getAdapterId() {
			return MockAbstractDataAdapter.ID;
		}

		/**
		 * 
		 * Return the adapter ID
		 * @return a unique identifier for this adapter
		 */
		@Override
		public boolean isSupported(Integer entry) {
			return true;
		}

		@Override
		public ByteArrayId getDataId(Integer entry) {
			return new ByteArrayId("DataID"+entry.toString());
		}

		@SuppressWarnings({
			"unchecked",
			"rawtypes"
		})
		@Override
		public FieldReader getReader(ByteArrayId fieldId) {
			if(fieldId.equals(INTEGER)) {
				return new IntReader();
			}
			else if( fieldId.equals(ID)) {
				return new StringReader();
			}
			return null;
		}

		@SuppressWarnings({
			"unchecked",
			"rawtypes"
		})
		@Override
		public FieldWriter getWriter(ByteArrayId fieldId) {
			if( fieldId.equals(INTEGER) ) {
				return new IntWriter();
			}
			else if( fieldId.equals(ID) ) {
				return new StringWriter();
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
					if (fieldValue.getId().equals(INTEGER)) {
						intValue = (Integer)fieldValue.getValue();
					}
					else if (fieldValue.getId().equals(	ID)) {
						id = (String) fieldValue.getValue();
					}
				}

				@Override
				public Integer buildRow(final ByteArrayId dataId ) {
					return new Integer(intValue);
				}
			};
		}
		
	}	// class MockAbstractDataAdapter

	
	//*************************************************************************
	//
	// Test index field type for dimension.
	//
	//*************************************************************************
	class TestIndexFieldType implements CommonIndexValue {
		private Integer indexValue;
		
		public TestIndexFieldType(Integer _indexValue) {
			this.indexValue = _indexValue;
		}
		
		@Override
		public byte[] getVisibility() {
			return null;
		}

		@Override
		public void setVisibility(byte[] visibility) {
		}
		
	}
	

	//*************************************************************************
	//
	// Test class that implements PersistentIndexFieldHandler<T> for 
	// instantiation of MockAbstractDataAdapter object.
	//
	//*************************************************************************
	private class TestPersistentIndexFieldHandler
			implements PersistentIndexFieldHandler<
					Integer,TestIndexFieldType,Object> {

		public TestPersistentIndexFieldHandler() {
		}

		@Override
		public ByteArrayId[] getNativeFieldIds() {
			return new ByteArrayId[] {MockAbstractDataAdapter.INTEGER};
		}

		// toIndexValue simply increments each digit in number.
		@Override
		public TestIndexFieldType toIndexValue(Integer row) {

			String sRow = row.toString();
			int numDigits = sRow.length();
			String sNewRow = new String();
			char[] newDigit = new char[1];
			for( int i=0 ; i<numDigits ; i++) {
				char digit = sRow.charAt(i);
				switch(digit) {
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
				sNewRow = sNewRow.concat(new String(newDigit));
			}
			return new TestIndexFieldType(Integer.decode(sNewRow));
		}

		// toNativeValues decrements each digit in the value.
		@SuppressWarnings("unchecked")
		@Override
		public PersistentValue<Object>[] toNativeValues(
				TestIndexFieldType _indexValue) {
			
			String sRow = _indexValue.indexValue.toString();
			int numDigits = sRow.length();
			String sNewRow = new String();
			char[] newDigit = new char[1];
			for( int i=0 ; i<numDigits ; i++) {
				char digit = sRow.charAt(i);
				switch(digit) {
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
				sNewRow = sNewRow.concat(new String(newDigit));
			}
			Integer newValue = Integer.decode(sNewRow);

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
		public void fromBinary(byte[] bytes) {
		}
		
	}
	

	//*************************************************************************
	//
	// Test class that implements NativeFieldHandler<RowType,FieldType> 
	// for instantiation of MockAbstractDataAdapter object.
	//
	//*************************************************************************
	class TestNativeFieldHandler 
			implements NativeFieldHandler<Integer,Object> {

		@Override
		public ByteArrayId getFieldId() {
			return MockAbstractDataAdapter.INTEGER;
		}

		@Override
		public Object getFieldValue(Integer row) {
			return row;
		}
		
	}
		

	//*************************************************************************
	//
	// Test implementation on interface DimensionField for use by 
	// TestIndexModel.
	//
	//*************************************************************************
	private class TestDimensionField implements DimensionField<TestIndexFieldType> {
		final ByteArrayId fieldId;

		public TestDimensionField() {
			fieldId = new ByteArrayId(new String("TestDimensionField1"));
		}
		
		@Override
		public double normalize(double value) {
			return 0;
		}

		@Override
		public BinRange[] getNormalizedRanges(NumericData range) {
			return null;
		}

		@Override
		public byte[] toBinary() {
			return null;
		}

		@Override
		public void fromBinary(byte[] bytes) {
		}

		@Override
		public NumericData getNumericData(TestIndexFieldType dataElement) {
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
		
	}
	
	
	//*************************************************************************
	//
	// Test index model class for use in testing encoding by 
	// AbstractDataAdapter.
	//
	//*************************************************************************
	private class TestIndexModel implements CommonIndexModel {

		private TestDimensionField[] dimensionFields;
		
		public TestIndexModel() {
			dimensionFields = new TestDimensionField[3];
			dimensionFields[0] = new TestDimensionField();
			dimensionFields[1] = new TestDimensionField();
			dimensionFields[2] = new TestDimensionField();
		}
		
		@Override
		public FieldReader<CommonIndexValue> getReader(ByteArrayId fieldId) {
			return null;
		}

		@Override
		public FieldWriter<Object, CommonIndexValue> getWriter(
				ByteArrayId fieldId) {
			return null;
		}

		@Override
		public byte[] toBinary() {
			return null;
		}

		@Override
		public void fromBinary(byte[] bytes) {
		}

		@Override
		public TestDimensionField[] getDimensions() {
			return this.dimensionFields;
		}

		@Override
		public String getId() {
			return null;
		}
		
	}	
	
	@Test
	//*************************************************************************
	//
	// Test encode(..) and decode(..) methods of AbstractDataAdapter via 
	// instantiation of MockAbstractDataAdapterTest. 
	//
	//*************************************************************************
	public void testAbstractDataAdapterEncodeDecode() {
		// To instantiate MockAbstractDataAdapter, need to create 
		// array of indexFieldHandlers and array of nativeFieldHandlers.
		ArrayList<
				PersistentIndexFieldHandler<
						Integer, 
						? extends CommonIndexValue, 
						Object
				>
		> indexFieldHandlers = 
				new ArrayList<
						PersistentIndexFieldHandler<
								Integer, 
								? extends CommonIndexValue, 
								Object
						>
				>();
		indexFieldHandlers.add(new TestPersistentIndexFieldHandler());

		ArrayList<NativeFieldHandler<Integer,Object>> nativeFieldHandlers =
				new ArrayList< NativeFieldHandler<Integer,Object>>();
		nativeFieldHandlers.add(new TestNativeFieldHandler());

		MockAbstractDataAdapter mockAbstractDataAdapter = 
				new MockAbstractDataAdapter(
						indexFieldHandlers,
						nativeFieldHandlers
				);
		TestIndexModel testIndexModel = new TestIndexModel();
		Integer beforeValue = 123456;
		AdapterPersistenceEncoding testEncoding = 
				mockAbstractDataAdapter.encode(beforeValue, testIndexModel);
		Integer afterValue = 
				mockAbstractDataAdapter.decode(testEncoding, testIndexModel);

		Assert.assertEquals(new String("EncodeDecode_test"), beforeValue, afterValue);
	}

	@Test
	public void testAbstractDataAdapterPersistance() {
		ArrayList<
		PersistentIndexFieldHandler<
		Integer, 
		? extends CommonIndexValue, 
				Object
				>
		> indexFieldHandlers = 
		new ArrayList<
		PersistentIndexFieldHandler<
		Integer, 
		? extends CommonIndexValue, 
				Object
				>
		>();
		indexFieldHandlers.add(new TestPersistentIndexFieldHandler());

		ArrayList<NativeFieldHandler<Integer,Object>> nativeFieldHandlers =
				new ArrayList< NativeFieldHandler<Integer,Object>>();
		nativeFieldHandlers.add(new TestNativeFieldHandler());

		MockAbstractDataAdapter mockAbstractDataAdapter = 
				new MockAbstractDataAdapter(
						indexFieldHandlers,
						nativeFieldHandlers
						);
		
		MockAbstractDataAdapter obj = PersistenceUtils.fromBinary(
				PersistenceUtils.toBinary(mockAbstractDataAdapter),
				MockAbstractDataAdapter.class);
		
		// TODO is there another test?
		Assert.assertNotNull(obj);
	}
}
