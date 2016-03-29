package mil.nga.giat.geowave.datastore.accumulo.util;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.datastore.accumulo.util.BitmaskUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.BitmaskedFieldInfoComparator;

import org.junit.Assert;
import org.junit.Test;

public class BitmaskUtilsTest
{
	final static BitSet zeroth = new BitSet();
	final static BitSet first = new BitSet();
	final static BitSet second = new BitSet();
	final static BitSet third = new BitSet();
	final static BitSet fourth = new BitSet();
	final static BitSet fifth = new BitSet();
	final static BitSet sixth = new BitSet();
	final static BitSet seventh = new BitSet();
	final static BitSet eighth = new BitSet();
	final static BitSet composite_0_1_2 = new BitSet();

	// generate bitsets
	static {
		zeroth.set(0);
		first.set(1);
		second.set(2);
		third.set(3);
		fourth.set(4);
		fifth.set(5);
		sixth.set(6);
		seventh.set(7);
		eighth.set(8);
		composite_0_1_2.set(0);
		composite_0_1_2.set(1);
		composite_0_1_2.set(2);
	}

	@Test
	public void testGenerateBitSet() {
		Assert.assertTrue(Arrays.equals(
				zeroth.toByteArray(),
				BitmaskUtils.generateBitmask(0)));
		Assert.assertTrue(Arrays.equals(
				eighth.toByteArray(),
				BitmaskUtils.generateBitmask(8)));
	}

	@Test
	public void testByteSize() {

		// confirm bitmasks are of correct (minimal) byte length
		Assert.assertTrue(1 == zeroth.toByteArray().length);
		Assert.assertTrue(2 == eighth.toByteArray().length);
	}

	@Test
	public void testGetOrdinal() {
		Assert.assertTrue(0 == BitmaskUtils.getOrdinal(zeroth.toByteArray()));
		Assert.assertTrue(1 == BitmaskUtils.getOrdinal(first.toByteArray()));
		Assert.assertTrue(8 == BitmaskUtils.getOrdinal(eighth.toByteArray()));
	}

	@Test
	public void testCompositeBitmask() {

		// generate composite bitmask for 3 bitmasks and ensure correctness
		final byte[] bitmask = BitmaskUtils.generateCompositeBitmask(Arrays.asList(
				zeroth.toByteArray(),
				first.toByteArray(),
				second.toByteArray()));
		Assert.assertTrue(BitSet.valueOf(
				bitmask).equals(
				composite_0_1_2));
	}

	@Test
	public void testDecompositionOfComposite() {

		// decompose composite bitmask and ensure correctness
		final List<byte[]> bitmasks = BitmaskUtils.getBitmasks(composite_0_1_2.toByteArray());
		Assert.assertTrue(bitmasks.size() == 3);
		Assert.assertTrue(Arrays.equals(
				zeroth.toByteArray(),
				bitmasks.get(0)));
		Assert.assertTrue(Arrays.equals(
				first.toByteArray(),
				bitmasks.get(1)));
		Assert.assertTrue(Arrays.equals(
				second.toByteArray(),
				bitmasks.get(2)));
	}

	@Test
	public void testCompositeSortOrder() {

		// generate meaningless fieldInfo to transform
		final FieldInfo<Object> original = new FieldInfo<Object>(
				new PersistentValue<Object>(
						null, // will be overwritten by
								// BitmaskUtils.transformField() below
						null), // unused in this instance
				null, // unused in this instance
				null); // unused in this instance

		// clone original fieldInfo overwriting dataValue.id with bitmask
		final FieldInfo<Object> field0 = BitmaskUtils.transformField(
				original,
				zeroth.toByteArray());
		final FieldInfo<Object> field1 = BitmaskUtils.transformField(
				original,
				first.toByteArray());
		final FieldInfo<Object> field2 = BitmaskUtils.transformField(
				original,
				second.toByteArray());
		final FieldInfo<Object> field3 = BitmaskUtils.transformField(
				original,
				third.toByteArray());
		final FieldInfo<Object> field4 = BitmaskUtils.transformField(
				original,
				fourth.toByteArray());
		final FieldInfo<Object> field5 = BitmaskUtils.transformField(
				original,
				fifth.toByteArray());
		final FieldInfo<Object> field6 = BitmaskUtils.transformField(
				original,
				sixth.toByteArray());
		final FieldInfo<Object> field7 = BitmaskUtils.transformField(
				original,
				seventh.toByteArray());
		final FieldInfo<Object> field8 = BitmaskUtils.transformField(
				original,
				eighth.toByteArray());

		// construct list in wrong order
		final List<FieldInfo<Object>> fieldInfoList = Arrays.asList(
				field8,
				field7,
				field6,
				field5,
				field4,
				field3,
				field2,
				field1,
				field0);

		// sort in place and ensure list sorts correctly
		Collections.sort(
				fieldInfoList,
				new BitmaskedFieldInfoComparator());

		Assert.assertTrue(field0.equals(fieldInfoList.get(0)));
		Assert.assertTrue(field1.equals(fieldInfoList.get(1)));
		Assert.assertTrue(field2.equals(fieldInfoList.get(2)));
		Assert.assertTrue(field3.equals(fieldInfoList.get(3)));
		Assert.assertTrue(field4.equals(fieldInfoList.get(4)));
		Assert.assertTrue(field5.equals(fieldInfoList.get(5)));
		Assert.assertTrue(field6.equals(fieldInfoList.get(6)));
		Assert.assertTrue(field7.equals(fieldInfoList.get(7)));
		Assert.assertTrue(field8.equals(fieldInfoList.get(8)));
	}

}