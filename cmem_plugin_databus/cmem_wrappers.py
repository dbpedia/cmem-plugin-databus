"""wrappers around some wrong/impractical cmem functions not deployed yet"""


from typing import Iterator
from cmem.cmempy.dp.proxy.graph import _get_graph_uri as cmem_get_graph_uri
from cmem.cmempy.dp.proxy.graph import get
from cmem.cmempy.api import request


def post_streamed_bytes(
    graph: str,
    data: Iterator[bytes],
    endpoint_id="default",
    replace=False,
    content_type="text/turtle",
):
    """Upload graph (streamed).

    Add the content of triple to a remote graph or replace the remote graph
    with the content of a triple file.

    Args:
        graph (str): The URI of the remote graph.
        data: content as BytesIO
        endpoint_id (str): Optional endpoint ID (always 'default').
        replace (bool): add (False) or replace (True)
        content_type (str): mime type of the file to post (default is turtle)

    Returns:
        requests.Response object

    """
    uri = cmem_get_graph_uri(endpoint_id, graph) + "&replace=" + str(replace).lower()
    headers = {"Content-Type": content_type}
    # https://2.python-requests.org/en/master/user/advanced/#streaming-uploads
    response = request(uri, method="POST", headers=headers, data=data, stream=True)
    return response


def get_streamed(
    graph,
    endpoint_id="default",
    owl_imports_resolution=False,
    accept="application/n-triples",
):
    """GET graph (streamed).

    same as get

    Args:
        graph (str): The URI of the requested graph.
        endpoint_id (str): Optional endpoint ID (always 'default').
        owl_imports_resolution: Optional request imported graph as well.
        accept: Optional mimetype to request.

    Returns:
        requests.Response object
    """
    return get(
        graph,
        endpoint_id=endpoint_id,
        owl_imports_resolution=owl_imports_resolution,
        accept=accept,
        stream=True,
    )
