"""
Build/refresh the Azure AI Search policy index from ai/rag/policies/*.md.

Creates a hybrid index (BM25 + vector + semantic reranker), chunks each policy doc,
embeds chunks with Azure OpenAI, and uploads them. Run in CI (infra pipeline) or
manually after editing a policy. Idempotent — re-running replaces the index.

  python -m ai.rag.index_policies
"""
from __future__ import annotations

import os
from pathlib import Path

from ai.rag.retriever import _embed, INDEX

POLICY_DIR = Path(__file__).parent / "policies"
CHUNK_CHARS = 1200
EMBED_DIM = 1536  # text-embedding-3-small


def _chunks(text: str, size: int = CHUNK_CHARS):
    paras, buf = text.split("\n\n"), ""
    for p in paras:
        if len(buf) + len(p) > size and buf:
            yield buf.strip()
            buf = ""
        buf += p + "\n\n"
    if buf.strip():
        yield buf.strip()


def _ensure_index():
    from azure.core.credentials import AzureKeyCredential
    from azure.search.documents.indexes import SearchIndexClient
    from azure.search.documents.indexes.models import (
        SearchIndex, SimpleField, SearchableField, SearchField,
        SearchFieldDataType, VectorSearch, HnswAlgorithmConfiguration,
        VectorSearchProfile, SemanticConfiguration, SemanticSearch,
        SemanticPrioritizedFields, SemanticField,
    )
    client = SearchIndexClient(
        os.environ.get("AZURE_AI_SEARCH_ENDPOINT") or os.environ["AZURE_SEARCH_ENDPOINT"],
        AzureKeyCredential(os.environ["AZURE_SEARCH_KEY"]))
    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, key=True),
        SimpleField(name="doc_id", type=SearchFieldDataType.String, filterable=True),
        SearchableField(name="title", type=SearchFieldDataType.String),
        SearchableField(name="content", type=SearchFieldDataType.String),
        SearchField(name="content_vector",
                    type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                    searchable=True, vector_search_dimensions=EMBED_DIM,
                    vector_search_profile_name="default"),
    ]
    vs = VectorSearch(
        algorithms=[HnswAlgorithmConfiguration(name="default")],
        profiles=[VectorSearchProfile(name="default", algorithm_configuration_name="default")],
    )
    semantic = SemanticSearch(configurations=[SemanticConfiguration(
        name="default",
        prioritized_fields=SemanticPrioritizedFields(
            title_field=SemanticField(field_name="title"),
            content_fields=[SemanticField(field_name="content")]))])
    client.create_or_update_index(SearchIndex(
        name=INDEX, fields=fields, vector_search=vs, semantic_search=semantic))


def build():
    from azure.core.credentials import AzureKeyCredential
    from azure.search.documents import SearchClient
    _ensure_index()
    docs = []
    for md in sorted(POLICY_DIR.glob("*.md")):
        text = md.read_text(encoding="utf-8")
        title = text.splitlines()[0].lstrip("# ").strip()
        for i, chunk in enumerate(_chunks(text)):
            docs.append({"id": f"{md.stem}-{i}", "doc_id": md.stem,
                         "title": title, "content": chunk,
                         "content_vector": _embed(chunk)})
    client = SearchClient(
        os.environ.get("AZURE_AI_SEARCH_ENDPOINT") or os.environ["AZURE_SEARCH_ENDPOINT"], INDEX,
        AzureKeyCredential(os.environ["AZURE_SEARCH_KEY"]))
    client.upload_documents(docs)
    print(f"indexed {len(docs)} chunks from {POLICY_DIR} into '{INDEX}'")


if __name__ == "__main__":
    build()
