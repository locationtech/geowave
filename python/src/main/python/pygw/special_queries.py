from pygw.config import config
from pygw.base_models import PyGwJavaWrapper, QueryInterface

class StatisticsQuery(QueryInterface):
  """
  Builds a statistics query for aggregate statistics.
  To create a StatisticsQuery, for extended_id is the ID of the statisticqs query,
  and type must either be 'bbox' 'time_range'
  """
  def __init__(self, extended_id, type):
    if type == "bbox":
      builder = config.MODULE__geotime_query.VectorStatisticsQueryBuilder.newBuilder().factory().bbox()
    elif type == "time_range":
      builder = config.MODULE__geotime_query.VectorStatisticsQueryBuilder.newBuilder().factory().timeRange()
    else:
      raise AttributeError("Invalid query type")

    builder.fieldName(extended_id)
    java_ref = builder.build()
    super().__init__(config.GATEWAY, java_ref)

  def get_extended_id(self):
    return self._java_ref.getExtendedId()

"""
NOTE: AGGREGATE QUERY IS NOT IMPLEMENTED
  Due to the rabbit hole of constructing authnetications, counts, and constraints
  and lack of time to learn and create each one 

"""
# class AggregateQuery(QueryInterface):
#     def __init__(self):

#       java_ref = config.MODULE__core_store.AggregationQueryBuilder.newBuilder()
#       super().__init__(config.GATEWAY, java_ref)

#     def count(self, *type_names):
#       n = len(type_names)
#       j_string_cls = config.GATEWAY.jvm.java.lang.String
#       j_string_arr = config.GATEWAY.new_array(j_string_cls, n)
#       for idx, name in enumerate(type_names):
#         j_string_arr[idx] = name

#       self._java_ref.count(j_string_arr)
    
#     def authentications(self, auths):
#       self._java_ref.setAuthorizations(auths)

#     def constraints(self, constraints):
#       self._java_ref.constraints(constraints)
    
#     def build(self):
#       self._java_ref = self._java_ref.build()
    