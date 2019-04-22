from pygw.config import config
from pygw.base_models import PyGwJavaWrapper

class StatisticsQuery(PyGwJavaWrapper):
  "type is 'bbox' or 'time_range'"
  def __init__(self, extended_id, type):
    if type == "bbox":
      builder = config.MODULE__geotime_query.VectorStatisticsQueryBuilder.newBuilder().factory().bbox()
    else:
      builder = config.MODULE__geotime_query.VectorStatisticsQueryBuilder.newBuilder().factory().timeRange()

    builder.fieldName(extended_id)
    java_ref = builder.build()
    super().__init__(config.GATEWAY, java_ref)

  def get_extended_id(self):
    return self._java_ref.getExtendedId()
    