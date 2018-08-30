package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import java.util.Objects;
import java.util.function.Supplier;

public class UDFRegistrySPI
{
	public static UDFNameAndConstructor[] getSupportedUDFs() {
		return new UDFNameAndConstructor[] {
			new UDFNameAndConstructor(
					new String[] {"GeomContains"},
					GeomContains::new),
			new UDFNameAndConstructor(
					new String[] {"GeomCovers"},
					GeomCovers::new),
			new UDFNameAndConstructor(
					new String[] {"GeomCrosses"},
					GeomCrosses::new),
			new UDFNameAndConstructor(
					new String[] {"GeomDisjoint"},
					GeomDisjoint::new),
			new UDFNameAndConstructor(
					new String[] {"GeomEquals"},
					GeomEquals::new),
			new UDFNameAndConstructor(
					new String[] {"GeomIntersects"},
					GeomIntersects::new),
			new UDFNameAndConstructor(
					new String[] {"GeomOverlaps" },
					GeomOverlaps::new),
			new UDFNameAndConstructor(
					new String[] {"GeomTouches"},
					GeomTouches::new),
			new UDFNameAndConstructor(
					new String[] {"GeomWithin"},
					GeomWithin::new),
			new UDFNameAndConstructor(
					new String[] {"GeomWithinDistance" },
					GeomWithinDistance::new)
		};
	}

	public static UDFNameAndConstructor findFunctionByName(
			String udfName ) {
		UDFNameAndConstructor[] udfFunctions = UDFRegistrySPI.getSupportedUDFs();
		for (int iUDF = 0; iUDF < udfFunctions.length; iUDF += 1) {
			UDFNameAndConstructor compare = udfFunctions[iUDF];
			if (compare.nameMatch(udfName)) {
				return compare;
			}
		}
		return null;
	}

	public static class UDFNameAndConstructor
	{
		private final String[] udfNames;
		private final Supplier<GeomFunction> predicateConstructor;

		public UDFNameAndConstructor(
				final String[] udfNames,
				final Supplier<GeomFunction> predicateConstructor ) {
			this.udfNames = udfNames;
			this.predicateConstructor = predicateConstructor;
		}

		public String[] getUDFNames() {
			return udfNames;
		}

		public boolean nameMatch(
				String udfName ) {
			for (int iName = 0; iName < udfNames.length; iName += 1) {
				if (Objects.equals(
						udfNames[iName],
						udfName)) {
					return true;
				}
			}
			return false;
		}

		public Supplier<GeomFunction> getPredicateConstructor() {
			return predicateConstructor;
		}
	}

}
