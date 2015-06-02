/**
 * 
 */
package mil.nga.giat.geowave.adapter.vector.query.hbase;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.FilterToCQLTool;
import mil.nga.giat.geowave.adapter.vector.query.hbase.generated.FilterProtos;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.hbase.entities.HBaseRowId;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.log4j.Logger;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author viggy This is a replacement for <code> CqlQueryFilterIterator </code>
 *         . This filter will run on Tablet Servers.
 */
public class CqlHBaseQueryFilter extends
		FilterBase
{

	private static Logger LOGGER = Logger.getLogger(CqlHBaseQueryFilter.class);
	private org.opengis.filter.Filter gtFilter;
	private CommonIndexModel model;

	private FeatureDataAdapter dataAdapter;

	public CqlHBaseQueryFilter(
			String gtFilterStr,
			byte[] modelBytes,
			byte[] dataAdapterBytes )
			throws CQLException {
		gtFilter = ECQL.toFilter(gtFilterStr);
		model = PersistenceUtils.fromBinary(
				modelBytes,
				CommonIndexModel.class);
		dataAdapter = PersistenceUtils.fromBinary(
				dataAdapterBytes,
				FeatureDataAdapter.class);
	}

	@Override
	public ReturnCode filterKeyValue(
			Cell v )
			throws IOException {
		if ((gtFilter != null) && (model != null) && (dataAdapter != null)) {
			final HBaseRowId rowId = new HBaseRowId(
					CellUtil.cloneRow(v));
			final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
			final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();

			// TODO #406 Need to fix this. Adding it currently to just fix
			// compilation issue due to merge with #238
			final PersistentDataset<byte[]> unknownData = new PersistentDataset<byte[]>();

			final ByteArrayId fieldId = new ByteArrayId(
					CellUtil.cloneQualifier(v));
			final FieldReader<? extends CommonIndexValue> reader = model.getReader(fieldId);
			if (reader == null) {
				// try extended data
				final FieldReader<Object> extReader = dataAdapter.getReader(fieldId);
				if (extReader == null) {
					return ReturnCode.NEXT_COL;
				}
				final Object fieldValue = extReader.readField(CellUtil.cloneValue(v));
				extendedData.addValue(new PersistentValue<Object>(
						fieldId,
						fieldValue));
			}
			else {
				final CommonIndexValue fieldValue = reader.readField(CellUtil.cloneValue(v));
				// TODO #406 Currently we are not supporting Column Visibility
				// fieldValue.setVisibility(key.getColumnVisibilityData().getBackingArray());
				commonData.addValue(new PersistentValue<CommonIndexValue>(
						fieldId,
						fieldValue));
			}
			final IndexedAdapterPersistenceEncoding encoding = new IndexedAdapterPersistenceEncoding(
					new ByteArrayId(
							rowId.getAdapterId()),
					new ByteArrayId(
							rowId.getDataId()),
					new ByteArrayId(
							rowId.getInsertionId()),
					rowId.getNumberOfDuplicates(),
					commonData,
					extendedData);
			final SimpleFeature feature = dataAdapter.decode(
					encoding,
					new Index(
							null,
							model));
			if (feature == null) {
				return ReturnCode.NEXT_COL;
			}
			return evaluateFeature(
					gtFilter,
					feature) ? ReturnCode.INCLUDE_AND_NEXT_COL : ReturnCode.NEXT_COL;
		}
		return defaultFilterResult() ? ReturnCode.INCLUDE_AND_NEXT_COL : ReturnCode.NEXT_COL;
	}

	public static Filter parseFrom(
			byte[] pbBytes )
			throws DeserializationException {
		mil.nga.giat.geowave.adapter.vector.query.hbase.generated.FilterProtos.CqlHBaseQueryFilter proto;
		try {
			proto = FilterProtos.CqlHBaseQueryFilter.parseFrom(pbBytes);
			return new CqlHBaseQueryFilter(
					proto.getGtFilter(),
					proto.getModel().toByteArray(),
					proto.getDataAdapter().toByteArray());
		}
		catch (InvalidProtocolBufferException | CQLException e) {
			throw new DeserializationException(
					e);
		}
	}

	public byte[] toByteArray() {
		FilterProtos.CqlHBaseQueryFilter.Builder builder = FilterProtos.CqlHBaseQueryFilter.newBuilder();
		if (gtFilter != null) builder.setGtFilter(FilterToCQLTool.toCQL(gtFilter));
		if (model != null) builder.setDataAdapter(ByteStringer.wrap(PersistenceUtils.toBinary(model)));
		if (dataAdapter != null) builder.setDataAdapter(ByteStringer.wrap(PersistenceUtils.toBinary(dataAdapter)));

		return builder.build().toByteArray();
	}

	protected boolean defaultFilterResult() {
		return true;
	}

	protected boolean evaluateFeature(
			final org.opengis.filter.Filter filter,
			final SimpleFeature feature ) {
		return filter.evaluate(feature);
	}

}
