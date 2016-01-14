package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

public class PersistenceUtilsTest
{

	public static class APersistable implements
			Persistable
	{

		@Override
		public byte[] toBinary() {
			return new byte[] {
				1,
				2,
				3
			};
		}

		@Override
		public void fromBinary(
				byte[] bytes ) {
			Assert.assertTrue(Arrays.equals(
					bytes,
					new byte[] {
						1,
						2,
						3
					}));

		}

	}

	@Test
	public void test() {
		APersistable persistable = new APersistable();
		Assert.assertTrue(PersistenceUtils.fromBinary(
				PersistenceUtils.toBinary(new ArrayList<Persistable>())).isEmpty());
		Assert.assertTrue(PersistenceUtils.fromBinary(
				PersistenceUtils.toBinary(Collections.<Persistable> singleton(persistable))).size() == 1);

		Assert.assertTrue(PersistenceUtils.fromBinary(
				PersistenceUtils.toBinary(Arrays.<Persistable> asList(new Persistable[] {
					persistable,
					persistable
				}))).size() == 2);
	}
}
