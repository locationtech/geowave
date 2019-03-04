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

    def get_indices(self):
        j_indices = self.interfacer.getIndices(self._java_ref)
        return [Index(self._gateway, j_index) for j_index in j_indices]

    def add_type(self, type_adapter, index):
        """NOTE: This is slightly different from java api. Currently does not support var-arg initial indices"""
        j_adapter = type_adapter._java_ref
        j_index = index._java_ref
        self.interfacer.addType(self._java_ref, j_adapter, j_index)

    def create_writer(self, type_adapter_name):
        j_writer = self._java_ref.createWriter(type_adapter_name)
        return Writer(self._gateway, j_writer)

class DataTypeAdapter(PyGwJavaWrapper):
    """Wrapper to expose all of DataTypeAdapter API"""
    # TODO: Implement API
    def get_type_name(self):
        return self._java_ref.getTypeName()

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