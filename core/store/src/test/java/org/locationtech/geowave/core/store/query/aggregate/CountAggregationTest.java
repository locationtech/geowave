package org.locationtech.geowave.core.store.query.aggregate;

import static org.junit.Assert.assertEquals;
import java.util.List;
import org.junit.Test;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;

public class CountAggregationTest extends AbstractCommonIndexAggregationTest<Persistable, Long> {

  @Test
  public void testCountAggregation() {
    final Long expectedCount = 42L;
    final List<CommonIndexedPersistenceEncoding> encodings =
        generateObjects(expectedCount.intValue());
    final Long result = aggregateObjects(new CountAggregation(), encodings);
    assertEquals(expectedCount, result);
  }

}
