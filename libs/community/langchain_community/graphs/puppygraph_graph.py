import json
from typing import Any, Dict, List

from langchain_community.graphs.graph_document import GraphDocument
from langchain_community.graphs.graph_store import GraphStore

_GREMLIN_STR = "gremlin"
_CYPHER_STR = "cypher"


class PuppyGraph(GraphStore):
    """PuppyGraph wrapper for graph operations.

    PuppyGraph can query *existing relational data stores*
    into a unified graph model with *Zero-ETL*.

    PuppyGraph supports both *Cypher* and *Gremlin* query languages.

    *Security note*: Make sure that the database connection uses credentials
        that are narrowly-scoped to only include necessary permissions.
        Failure to do so may result in data corruption or loss, since the calling
        code may attempt commands that would result in deletion, mutation
        of data if appropriately prompted or reading sensitive data if such
        data is present in the database.
        The best way to guard against such negative outcomes is to (as appropriate)
        limit the permissions granted to the credentials used with this tool.

        See https://python.langchain.com/docs/security for more information.
    """

    def __init__(
            self,
            query_language: str = "gremlin",
            ip_address: str = "127.0.0.1",
            username: str = "puppygraph",
            password: str = "puppygraph123",
    ) -> None:
        """Create a new PuppyGraph graph wrapper instance.

        Args:
            query_language (str): The query language to use. Defaults to 'gremlin', can be 'gremlin' or 'cypher'.
            ip_address (str): The IP address of the PuppyGraph server.
            username (str): The username for authentication.
            password (str): The password for authentication.
        """
        try:
            from puppygraph import PuppyGraphClient, PuppyGraphHostConfig
        except ImportError as e:
            raise ImportError(
                "Please install puppygraph first: `pip3 install puppygraph`"
            ) from e

        if query_language not in [_GREMLIN_STR, _CYPHER_STR]:
            raise ValueError(
                f"Query language must be either {_GREMLIN_STR} or {_CYPHER_STR}."
            )

        self._query_language = query_language
        self._client = PuppyGraphClient(
            config=PuppyGraphHostConfig(
                ip=ip_address, username=username, password=password
            )
        )

        self._schema_json_str: str = self._client.get_schema()

    @property
    def get_schema(self) -> str:
        """Return the schema of the Graph database"""
        return self._schema_json_str

    @property
    def get_structured_schema(self) -> Dict[str, Any]:
        """Return the schema of the Graph database"""
        return _to_structured_schema(self._schema_json_str)

    def query(self, query: str, params: dict = {}) -> List[Dict[str, Any]]:
        """Query the graph."""
        if self._query_language == _GREMLIN_STR:
            return self._client.gremlin_query(query=query)
        elif self._query_language == _CYPHER_STR:
            return self._client.cypher_query(query=query, params=params)
        else:
            raise ValueError(
                f"Query language must be either {_GREMLIN_STR} or {_CYPHER_STR}."
            )

    def refresh_schema(self) -> None:
        """Refresh the graph schema information."""
        self._schema_json_str = self._client.get_schema()

    def add_graph_documents(
            self, graph_documents: List[GraphDocument], include_source: bool = False
    ) -> None:
        raise NotImplementedError(
            "Please add the node or relationships directly in the corresponding relational database."
        )


def _to_structured_schema(schema_json_str: str) -> Dict[str, Any]:
    """Convert the schema JSON string to a structured schema."""
    schema_dict = json.loads(schema_json_str)
    return {
        "node_props": {
            vertex["label"]: [
                {
                    "property": attr["name"],
                    "type": attr["type"],
                }
                for attr in vertex.get("attributes", [])
            ]
            for vertex in schema_dict["vertices"]
        },
        "rel_props": {
            edge["label"]: [
                {
                    "property": attr["name"],
                    "type": attr["type"],
                }
                for attr in edge.get("attributes", [])
            ]
            for edge in schema_dict["edges"]
            if edge.get("attributes")
        },
        "relationships": [
            {
                "start": edge["from"],
                "end": edge["to"],
                "type": edge["label"],
            }
            for edge in schema_dict["edges"]
        ],
    }