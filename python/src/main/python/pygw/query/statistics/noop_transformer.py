from ..query_result_transformer import QueryResultTransformer
#from py4j.java_gateway import JavaGateway


class NoOpTransformer(QueryResultTransformer):
    """
    Transforms Java Long into a pyhton
    results.  In order to accomplish this, the pygw variant of the SimpleFeatureType
    has to be constructed from the feature.  In order to avoid doing this for every
    result, there is a feature type cache that the transform function can pull from.
    """

    def transform(self, j_result):
        return j_result
