from pygw.config import config
from pygw.base_models import PyGwJavaWrapper

class Query(PyGwJavaWrapper):
    """ NOTE: This utilizes a factory pattern"""

    def __init__(self, java_ref):
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
            qb.set_limit(limit)
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

    @classmethod
    def cql(cls, cql_query, index=None, auths=None, limit=None, type_names=None, which_fields=None):
        qb = VectorQueryBuilder()
        qb.set_cql_constraint(cql_query)

        if index:
            qb.set_index(index)
        if auths:
            qb.set_auth(auths)
        if limit:
            qb.set_limit(limit)
        if type_names:
            qb.set_type_names(type_names)
        if which_fields:
            qb.set_fields(which_fields)

        return cls(qb.build())

class QueryBuilder(PyGwJavaWrapper):
    """
    [INTERNAL]
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
            for t in type_names: assert isinstance(t, str)
            n = len(type_names)
            j_string_cls = config.GATEWAY.jvm.java.lang.String
            j_string_arr = config.GATEWAY.new_array(j_string_cls, n)
            for idx, name in enumerate(type_names):
                j_string_arr[idx] = name
            self.type_names = type_names
            self._java_ref.setTypeNames(j_string_arr)

    def set_constraint(self, constraint=None):
        if constraint:
            # NOTE: Might have to introduce a constraint maker. These are more complex than anticipated.
            # For now, just creating the base constraints with no opts
            try:
                j_constraint = QueryBuilder.VALID_CONSTRAINTS[constraint]()
                self._java_ref.constraints(j_constraint)
            except KeyError:
                raise QueryBuilder.InvalidConstraintError("No constraint named {}".format(constraint))

    def set_auth(self, auths=None):
        if auths:
            assert isinstance(auths, list)
            for a in auths: assert isinstance(a, str)
            n = len(auths)
            j_string_cls = config.GATEWAY.jvm.java.lang.String
            j_string_arr = config.GATEWAY.new_array(j_string_cls, n)
            for idx, auth in enumerate(auths):
                j_string_arr[idx] = auth
            self._java_ref.setAuthorizations(j_string_arr)
            
    def set_limit(self, limit=None):
        if limit:
            assert isinstance(limit, int)
            self._java_ref.limit(limit)
    
    def set_fields(self, fields=None):
        if not hasattr(self, 'type_names'):
            raise QueryBuilder.IncompatibleOptions(\
                 "Subset fields can only be applied when exactly ONE type name is set: You have 0 set.")
        if len(self.type_names) != 1:
            raise QueryBuilder.IncompatibleOptions(\
                "Subset fields can only be applied when exactly ONE type name is set: You have {} set.".format(len(self.type_names)))
        if fields:
            assert isinstance(fields, list)
            for f in fields: assert isinstance(f, str)
            n = len(fields)
            j_string_cls = config.GATEWAY.jvm.java.lang.String
            j_string_arr = config.GATEWAY.new_array(j_string_cls, n)
            for idx, field in enumerate(fields):
                j_string_arr[idx] = field
            self._java_ref.subsetFields(self.type_names[0], j_string_arr)
            
    def set_index(self, index=None):
        if index:
            self._java_ref.indexName(index)

    def build(self):
        """Builds a query."""
        return self._java_ref.build()

    # For now just doing those in query constraints
    # TODO: Refactor - A better way to do this would probably involve using the Singleton ConstraintsFactory in QueryBuilder.
    VALID_CONSTRAINTS = {
        "everything" : lambda : config.MODULE__query_constraints.EverythingQuery(),

        # TODO - Looks like constructor takes add'l opts ... Investigate
        "basic" : lambda : config.MODULE__query_constraints.BasicQuery(),

        # TODO - Looks like constructor takes some add'l opts
        "coordinate_range" : lambda: config.MODULE__query_constraints.CoordinateRangeQuery(),

        # TODO - Looks like constructor takes some add'l opts
        "data_id" : lambda: config.MODULE__query_constraints.DataIdQuery(),

        # TODO - Looks like constructor takes some add'l opts
        "insertion_id" : lambda: config.MODULE__query_constraints.InsertionIdQuery(),

         # TODO - Looks like constructor takes some add'l opts
        "prefix_id" : lambda: config.MODULE__query_constraints.PrefixIdQuery(),
    }

    class InvalidConstraintError(Exception): pass
    class IncompatibleOptions(Exception): pass

class VectorQueryBuilder(QueryBuilder):
    """QueryBuilder for vector (SimpleFeature) data."""

    def __init__(self):
        j_vector_qbuilder = config.MODULE__geotime_query.VectorQueryBuilder.newBuilder()
        PyGwJavaWrapper.__init__(self, config.GATEWAY, j_vector_qbuilder)
        
    def set_cql_constraint(self, cql_query_string):
        j_vector_query_constraints_factory = self._java_ref.constraintsFactory()
        j_cql_constraint = j_vector_query_constraints_factory.cqlConstraints(cql_query_string)
        self._java_ref.constraints(j_cql_constraint)