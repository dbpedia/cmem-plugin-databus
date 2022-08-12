from cmem.cmempy.dp.proxy.graph import _get_graph_uri as cmem_get_graph_uri
from cmem.cmempy.api import request
from io import BytesIO


def post_streamed_bytesio(
        graph: str,
        data: BytesIO,
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
    response = request(uri, method="POST", headers=headers, data=data)
    return response


def get_clock(counter: int) -> str:
    """returns a clock symbol"""
    clock = {
        0: "🕛",
        1: "🕐",
        2: "🕑",
        3: "🕓",
        4: "🕔",
        5: "🕕",
        6: "🕖",
        7: "🕗",
        8: "🕘",
        9: "🕚",
    }
    return clock[int(repr(counter)[-1])]
