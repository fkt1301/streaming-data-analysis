import requests


def fetch_latest_schema(schema_registry_url: str, subject: str) -> str:
    """
    Fetch the latest registered Avro schema string for a subject.

    Subject naming follows Confluent convention: '<topic>-value' for the
    value schema of a topic (see schema_registry_url/subjects for all
    registered subjects).
    """
    response = requests.get(
        f"{schema_registry_url}/subjects/{subject}/versions/latest",
        timeout=10,
    )
    response.raise_for_status()
    return response.json()["schema"]
