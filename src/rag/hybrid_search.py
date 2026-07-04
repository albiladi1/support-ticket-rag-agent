"""
Deliverable 3 (part of): Hybrid search.

Combines BM25 (exact keyword matching — great for ticket IDs, product
codes, error messages) with dense vector search (semantic matching — great
for paraphrased questions), merged with Reciprocal Rank Fusion (RRF) as
covered in the Day 3 material.
"""
from rank_bm25 import BM25Okapi

from vector_index import load_index, vector_search


def _tokenize(text: str) -> list[str]:
    return text.lower().split()


def build_bm25(metadata: list[dict]) -> BM25Okapi:
    corpus = [_tokenize(chunk["text"]) for chunk in metadata]
    return BM25Okapi(corpus)


def bm25_search(query: str, top_k: int = 10) -> list[dict]:
    _, metadata = load_index()
    bm25 = build_bm25(metadata)
    scores = bm25.get_scores(_tokenize(query))

    ranked = sorted(
        zip(metadata, scores), key=lambda pair: pair[1], reverse=True
    )[:top_k]

    return [{**chunk, "bm25_score": float(score)} for chunk, score in ranked]


def reciprocal_rank_fusion(
    result_lists: list[list[dict]], key: str = "chunk_id", k: int = 60
) -> list[dict]:
    """Merges multiple ranked result lists into a single fused ranking."""
    fused_scores: dict[str, float] = {}
    chunk_lookup: dict[str, dict] = {}

    for results in result_lists:
        for rank, item in enumerate(results):
            cid = item[key]
            chunk_lookup[cid] = item
            fused_scores[cid] = fused_scores.get(cid, 0.0) + 1.0 / (k + rank + 1)

    fused = sorted(fused_scores.items(), key=lambda x: x[1], reverse=True)
    return [{**chunk_lookup[cid], "rrf_score": score} for cid, score in fused]


def hybrid_search(query: str, top_k: int = 10) -> list[dict]:
    vector_results = vector_search(query, top_k=top_k)
    keyword_results = bm25_search(query, top_k=top_k)
    return reciprocal_rank_fusion([vector_results, keyword_results])[:top_k]