"""
RAG retrieval over business policies + KPI definitions using **Azure AI Search**
(hybrid: vector + BM25 + semantic reranker). Returns cited chunks so policy answers
are grounded and auditable.

Env (from Key Vault via managed identity): AZURE_SEARCH_ENDPOINT, AZURE_SEARCH_KEY,
AZURE_SEARCH_INDEX, plus the embedding deployment for the vector query.
"""
from __future__ import annotations

import os

INDEX = os.environ.get("AZURE_SEARCH_INDEX", "investsphere-policies")
TOP_K = 4


def _embed(text: str) -> list[float]:
    from openai import AzureOpenAI
    client = AzureOpenAI(
        azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        api_key=os.environ["AZURE_OPENAI_API_KEY"],
        api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-10-21"),
    )
    dep = os.environ.get("AZURE_OPENAI_EMBED_DEPLOYMENT", "text-embedding-3-small")
    return client.embeddings.create(model=dep, input=text).data[0].embedding


def search_policy_docs(question: str, top_k: int = TOP_K) -> dict:
    """Hybrid search the policy index; return grounded chunks with citations."""
    from azure.core.credentials import AzureKeyCredential
    from azure.search.documents import SearchClient
    from azure.search.documents.models import VectorizedQuery

    client = SearchClient(
        endpoint=(os.environ.get("AZURE_AI_SEARCH_ENDPOINT") or os.environ["AZURE_SEARCH_ENDPOINT"]),
        index_name=INDEX,
        credential=AzureKeyCredential(os.environ["AZURE_SEARCH_KEY"]),
    )
    vector = VectorizedQuery(vector=_embed(question), k_nearest_neighbors=top_k,
                             fields="content_vector")
    results = client.search(
        search_text=question,                 # BM25 leg of the hybrid query
        vector_queries=[vector],              # vector leg
        query_type="semantic",               # semantic reranker
        semantic_configuration_name="default",
        top=top_k,
        select=["doc_id", "title", "content"],
    )
    chunks = [{"doc_id": r["doc_id"], "title": r["title"], "content": r["content"],
               "score": r.get("@search.score")} for r in results]
    return {"mart": "azure_ai_search:" + INDEX,
            "citations": [c["title"] for c in chunks],
            "chunks": chunks}


RAG_TOOL_SPEC = {
    "type": "function",
    "function": {
        "name": "search_policy_docs",
        "description": ("Retrieve business policy / KPI-definition text (leasing, "
                        "investment risk, revenue management, maintenance SLA, guest "
                        "experience, marketing, governance). Use for 'according to "
                        "policy…' or KPI-definition questions; cite the returned title."),
        "parameters": {"type": "object", "properties": {
            "question": {"type": "string", "description": "The policy/KPI question."}},
            "required": ["question"]},
    },
}
