# These classes define methods for interfacing with the
# abstract Classes/ Interfaces as defined in the core.store.api
# No Java objects should be instantiated here.
class PyGwJavaWrapper:
    """[INTERNAL] Base Class for all PyGw Objects that wrap py4j objects"""

    def __init__(self, gateway, java_ref):
        self._gateway = gateway
        self._java_ref = java_ref

    def __repr__(self):
        return "PyGW {} Object with JavaRef@{}".format(self.__class__, self._java_ref)

    def __eq__(self, other):
        if not isinstance(other, PyGwJavaWrapper):
            return False
        return self._java_ref == other._java_ref
    
    def is_instance_of(self, java_class):
        return isinstance(self._gateway, self._java_ref, java_class)

class DataStore(PyGwJavaWrapper):
    """Wrapper to expose all of DataStore API"""

    def __init__(self, gateway, java_ref):
        super().__init__(gateway, java_ref)
        self.interfacer = gateway.entry_point.storeInterfacer

    def get_indices(self, type_name=None):
        if type_name:
            j_indices = self.interfacer.getIndices(self._java_ref, type_name)
        else:
            j_indices = self.interfacer.getIndices(self._java_ref)
        return [Index(self._gateway, j_index) for j_index in j_indices]

    def add_type(self, type_adapter, index):
        # NOTE: This is slightly different from java api. Currently does not support var-arg initial indices
        # TODO: Fix
        j_adapter = type_adapter._java_ref
        j_index = index._java_ref
        self.interfacer.addType(self._java_ref, j_adapter, j_index)

    def create_writer(self, type_adapter_name):
        j_writer = self._java_ref.createWriter(type_adapter_name)
        return Writer(self._gateway, j_writer)

    def ingest(self, url, *indices, ingest_options=None):
        # TODO
        raise NotImplementedError
    
    def query(self, q):
        assert isinstance(q, QueryInterface)
        j_query = q._java_ref
        return self._java_ref.query(j_query)

    def aggregate(self, q):
        # TODO
        raise NotImplementedError
    
    def get_types(self):
        j_adapter_arr = self._java_ref.getTypes()
        return [DataTypeAdapter(self._gateway, j_adpt) for j_adpt in j_adapter_arr]

    def query_statistics(self, q):
        # TODO
        raise NotImplementedError

    def aggregate_statistics(self, q):
        # TODO
        raise NotImplementedError

    def copy_to(self, other, q=None):
        # TODO
        raise NotImplementedError
    
    def remove_index(self, index_name, type_name=None):
        # TODO
        raise NotImplementedError
    
    def remove_type(self, type_name):
        # TODO
        raise NotImplementedError
    
    def delete(self, q):
        # TODO
        raise NotImplementedError

    def delete_all(self, q):
        # TODO
        raise NotImplementedError

class DataTypeAdapter(PyGwJavaWrapper):
    """Wrapper to expose all of DataTypeAdapter API"""
 
    def get_type_name(self):
        return self._java_ref.getTypeName()

    # TODO: Are the rest of the API methods as defined in the Java interface
    # necessary to be wrapped in Python?

class Index(PyGwJavaWrapper):
    """Wrapper to expose all of Index API"""
    def get_name(self):
        return self._java_ref.getName()
    
    def get_index_strategy(self):
        j_obj = self._java_ref.getIndexStrategy()
        return j_obj.getClass().toString()

    def get_index_model(self):
        j_obj = self._java_ref.getIndexModel()
        return j_obj.getClass().toString()

class Writer(PyGwJavaWrapper):
    """Wrapper to expose all of Writer API"""
    def __init__(self, gateway, java_ref):
        super().__init__(gateway, java_ref)
        self.is_open = True

    def write(self, data):
        if not self.is_open:
            raise RuntimeError("Writer is already closed!")
        self._java_ref.write(data)
    
    def close(self):
        if self.is_open:
            self._java_ref.close()
            self.is_open = False
    # Might want to introduce a method/flag here that bulk writes in java.
    # Ex. give it a list of a data and it calls a java endpoint to do all the writing and closing.
    # Current pipeline will make N py4j calls for an N-element ingest

class QueryInterface(PyGwJavaWrapper):
    # TODO: Just describe its properties
    
    def describe(self):
        print("I'm a query! ... Please implement a better description for me...")