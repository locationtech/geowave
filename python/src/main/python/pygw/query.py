from pygw.config import config
from pygw.base_models import QueryBuilderInterface, QueryInterface

class QueryBuilder(QueryBuilderInterface):
    """
    NOTE: this might never have to be exposed to end-user 
            - Question: should this be consolidated with query?

    Necessary for a query:
    - Constraints (default: Everything)
    - Index: either all or single (default: All)
    - Authorizations: string array (default: empty string array)
    - Limits (default: null)
    - Fields: all fields or a subset (only 1 type-name if subset; default all)
    - Type names: all or a subset (default: all)
    - Hints(??)
    """
    def __init__(self):
        j_qbuilder = config.MODULE__core_store.QueryBuilder.newBuilder()
        super().__init__(config.GATEWAY, j_qbuilder)

    def set_type_names(self, type_names=None):
        if type_names:
            assert isinstance(type_names, list)
            n = len(type_names)
            j_string_cls = config.GATEWAY.jvm.java.lang.String
            j_string_arr = config.GATEWAY.new_array(j_string_cls, n)
            for idx, name in enumerate(type_names):
                j_string_arr[idx] = name
            self._java_ref.setTypeNames(j_string_arr)

    def set_constraint(self, constraint=None):
        if constraint:
            pass

    def set_auth(self, auths=None):
        if auths:
            pass
    
    def set_limits(self, limit=None):
        if limit:
            pass
    
    def set_fields(self, fields=None):
        if fields:
            pass
    
    def set_index(self, index=None):
        if index:
            self._java_ref.indexName(index)

    def build(self):
        return self._java_ref.build()

class Query(QueryInterface):
    """ NOTE: This utilizes a factory pattern"""

    def __init__(self, java_ref=None):
        super().__init__(config.GATEWAY, java_ref)

    @classmethod
    def build(cls, constraint=None, index=None, auths=None, limit=None, type_names=None, which_fields=None):
        """
        Build a new query.
        """
        qb = QueryBuilder()

        if constraint:
            qb.set_constraint(constraint)
        if index:
            qb.set_index(index)
        if auths:
            qb.set_auth(auths)
        if limit:
            qb.set_limits(limit)
        if type_names:
            qb.set_type_names(type_names)
        if which_fields:
            qb.set_fields(which_fields)

        return cls(qb.build())

    @classmethod
    def everything(cls):
        """
        Creates an everything-query.
        Effectively the same as `Query.build()`.
        """
        return cls.build()