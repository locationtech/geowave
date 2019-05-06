from pygw.config import config

# These classes define methods for interfacing with the
# abstract Classes/ Interfaces as defined in the core.store.api
# No Java objects should be instantiated here.
class PyGwJavaWrapper:
    """
    Base Class for PyGw Objects that wrap Java Objects.
    """

    def __init__(self, gateway, java_ref):
        """
        Args:
            gateway (py4j.java_gateway.JavaGateway) : Py4J Gateway connection to JVM
            java_ref (py4j.JavaObject) : Wrapped Java Object
        """
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
    """
    This class models the DataStore interface methods.
    """

    def __init__(self, gateway, java_ref):
        super().__init__(gateway, java_ref)

    def get_indices(self, type_name=None):
        """
        Get the indices that have been registered with this data store for a given type.

        Gets all registered indices if `type_name` is None.

        Args:
            type_name (str): The name of the type
        Returns:
            list of pygw.base_models.Index
        """

        if type_name:
            j_indices = self._java_ref.getIndices(type_name)
        else:
            j_indices = self._java_ref.getIndices()
        return [Index(self._gateway, j_index) for j_index in j_indices]

    def copy_to(self, other, q=None):
        """
        Copy data from this data store to another.

        All data is copied if `q` is None, else only the data queried by `q`.

        Args:
            other (pygw.base_models.DataStore) : The data store copied to
            q (pygw.base_models.QueryInterface) : Query filter for data to be copied
        """
        assert isinstance(other, DataStore)

        if q: 
            assert isinstance(q, QueryInterface)
            q = q._java_ref

        self._java_ref.copyTo(other._java_ref,q)

    def add_index(self, type_name, *indices):
        """
        Add new indices for a given data type. 

        Args:
            type_name (str) : name of data type to register indices to
            *indices (pygw.base_models.Index) : Index to add
        """
        assert isinstance(type_name,str)

        n = len(indices)
        j_index_class = config.MODULE__core_store.Index
        j_index_arr = config.GATEWAY.new_array(j_index_class,n)
        for idx, py_obj in enumerate(indices):
                j_index_arr[idx] = py_obj._java_ref
    
        self._java_ref.addType(type_name,j_index_arr)

    def remove_index(self, index_name, type_name=None):
        """
        Remove an index for a given data type. 

        If type_name is None, the specified index is removed for all types.

        Args:
            index_name (str) : name of the index to be removed.
            type_name (str) : name of data type to remove
        """
        if type_name:
            self._java_ref.removeIndex(index_name,type_name)
        else:
            self._java_ref.removeIndex(index_name)


    def remove_type(self, type_name):
        """
        Remove a type from the data store.

        Args:
            type_name (str) : name of the data type
        """
        assert isinstance(type_name, str)

        self._java_ref.removeType(type_name)
    
 
    def delete(self, q):
        """
        Delete all data in this data store that matches the query parameter.

        Args:
            q (pygw.base_models.QueryInterface) : the query criterion to use for deletion
        Returns:
            bool: True on success, False on fail
        """
        assert isinstance(q,QueryInterface)

        return self._java_ref.delete(q._java_ref)


    def delete_all(self):
        """
        Delete ALL data and ALL metadata for this datastore.
        """
        
        return self._java_ref.deleteAll()

    def create_writer(self, type_adapter_name):
        """
        Returns an index writer to perform batched write operations for the given data type name.
        
        Assumes the type has already been used previously or added using `add_type` and assumes one or
        more indices have been provided for this type.

        Args:
            type_name (str) : the type name
        Returns:
            a pygw.base_models.Writer, which can be used to write entries into this datastore of the given type 
        """    
        j_writer = self._java_ref.createWriter(type_adapter_name)

        if j_writer is None:
            return None

        return Writer(self._gateway, j_writer)

    # TODO: Implement API
    def get_type_name(self):
        return self._java_ref.getTypeName()

    def get_name(self):
        return self._java_ref.getName()


    def add_type(self, type_adapter, *initial_indices):
        """
        Add this type to the data store. This only needs to be called one time per type.

        Args:
            type_adapter (pygw.base_models.DataTypeAdapter): the data type adapter for this type that is used to read and write GeoWave entries
            initial_indices (pygw.base_models.Index): the initial indexing for this type, additional indices can be added in the future
        """
        assert isinstance(type_adapter,DataTypeAdapter)

        n = len(initial_indices)
        j_index_class = config.MODULE__core_store.Index
        j_index_arr = config.GATEWAY.new_array(j_index_class,n)
        for idx, py_obj in enumerate(initial_indices):
                j_index_arr[idx] = py_obj._java_ref
    
        self._java_ref.addType(type_adapter._java_ref,j_index_arr)

    def ingest(self, url, *indices, ingest_options=None):
        """
        Ingest from URL. 
        
        If this is a directory, this method will recursively search for valid files to
        ingest in the directory. This will iterate through registered IngestFormatPlugins to find one
        that works for a given file. 

        Args:
            url (str) : The URL for data to read and ingest into this data store
            *indices (pygw.base_models.Index) : Index to ingest into
            ingest_options : Options for ingest
        """ 
        # TODO: Ingest Options
        if ingest_options: raise NotImplementedError()

        # TODO: Not Fully implemented!

        assert isinstance(url,str)

        n = len(indices)
        j_index_class = config.MODULE__core_store.Index
        j_index_arr = config.GATEWAY.new_array(j_index_class,n)
        for idx, name in enumerate(indices):
                j_index_arr[idx] = name._java_ref
        java_url = config.GATEWAY.jvm.java.net.URL(url)
        self._java_ref.ingest(java_url,ingest_options,j_index_arr)
    
    def query(self, q):
        """
        Query this data store.

        Args:
            q (pygw.base_models.QueryInterface) : query
        Returns:
            an iterable of results
        """
        assert isinstance(q, QueryInterface)
        j_query = q._java_ref
        return self._java_ref.query(j_query)

    """
    Note: aggregateQuery umimplemented ATM. 
    """
    def aggregate(self, q):
        return self._java_ref.aggregate(q._java_ref)
    
    def get_types(self):
        """
        Get the names of registered types for this data store.
        
        Returns:
            list of str of registered type names
        """
        j_adapter_arr = self._java_ref.getTypes()
        return [DataTypeAdapter(self._gateway, j_adpt) for j_adpt in j_adapter_arr]

    def query_statistics(self, q):
        # TODO
        raise NotImplementedError

    """
    Get a single statistical result that matches the given query criteria
   
    Args:
        q (pygw.special_queries.StatisticsQuery): query the query criteria, use StatisticsQueryBuilder or its extensions and if you're
           interested in a particular common statistics type use StatisticsQueryBuilder.factory()
    Returns:
        If the query does not define that statistics type it will return null as aggregation
            only makes sense within a single type, otherwise aggregates the results of the query
            into a single result that is returned
    """
    def aggregate_statistics(self, q):
        return self._java_ref.aggregateStatistics(q._java_ref)


class DataTypeAdapter(PyGwJavaWrapper):
    """
    Models a DataTypeAdapter.
    """
 
    def get_type_name(self):
        """Get the name of the registered type name."""

        return self._java_ref.getTypeName()

    # TODO: Are the rest of the API methods as defined in the Java interface necessary to be wrapped in Python?

class Index(PyGwJavaWrapper):
    """
    Models an Index.
    """

    def get_name(self):
        """Get the name of the index."""

        return self._java_ref.getName()
    
    def get_index_strategy(self):
        """Get the strategy name for this Index."""

        j_obj = self._java_ref.getIndexStrategy()
        return j_obj.getClass().toString()

    def get_index_model(self):
        """Get the model name of this Index."""
        j_obj = self._java_ref.getIndexModel()
        return j_obj.getClass().toString()


class Writer(PyGwJavaWrapper):
    """
    Models a Writer
    """

    def __init__(self, gateway, java_ref):
        super().__init__(gateway, java_ref)
        self.is_open = True


    def write(self, data):
        """
        Write data into the associated data store.

        Args:
            data (py4j.JavaObject OR pygw.base_models.PyGwJavaWrapper) : data to be written

        Raises:
            RuntimeError: 'Writer is already closed' If writer is closed
        """
        if not self.is_open:
            raise RuntimeError("Writer is already closed!")
        
        if isinstance(data, PyGwJavaWrapper):
            data = data._java_ref
        
        self._java_ref.write(data)

    def close(self):
        """
        Close the writer.
        """
        if self.is_open:
            self._java_ref.close()
            self.is_open = False

class QueryInterface(PyGwJavaWrapper):
    pass
