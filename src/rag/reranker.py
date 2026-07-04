"""
Deliverable 3 (part of): Reranking — Stage 2 precision gate.

A true cross-encoder model (e.g. sentence-transformers CrossEncoder) is the
production-grade choice and is used by default here. As a lightweight
zero-extra-dependency fallback, an LLM relevance-scoring reranker is also
provided; swap RERANK_METHOD to switch.
"""
import os

RERANK_METHOD = os.getenv("RERANK_METHOD", "cross_encoder")  # or "llm"

_cross_encoder = None


def _get_cross_encoder():
    global _cross_encoder
    if _cross_encoder is None:
        from sentence_transformers import CrossEncoder
        _cross_encoder = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
    return _cross_encoder


def rerank(query: str, candidates: list[dict], top_k: int = 5) -> list[dict]:
    if not candidates:
        return []

    if RERANK_METHOD == "cross_encoder":
        model = _get_cross_encoder()
        pairs = [(query, c["text"]) for c in candidates]
        scores = model.predict(pairs)
        for c, s in zip(candidates, scores):
            c["rerank_score"] = float(s)
        ranked = sorted(candidates, key=lambda c: c["rerank_score"], reverse=True)
        return ranked[:top_k]

    # Fallback: LLM-as-reranker (see agent.py for the LLM call helper)
    from agent import call_llm

    scored = []
    for c in candidates:
        prompt = (
            "Rate how relevant this passage is to the query on a scale of 0-10. "
            "Answer with only the number.\n\n"
            f"Query: {query}\nPassage: {c['text']}"
        )
        try:
            score = float(call_llm(prompt, max_tokens=5).strip())
        except (ValueError, TypeError):
            score = 0.0
        c["rerank_score"] = score
        scored.append(c)

    return sorted(scored, key=lambda c: c["rerank_score"], reverse=True)[:top_k]