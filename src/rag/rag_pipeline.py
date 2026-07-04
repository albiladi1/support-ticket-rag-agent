"""
Deliverable 3: Full RAG pipeline glue.

index_gold_zone(): reads Gold Delta table -> chunks -> embeds -> builds
                    the vector index (run after delta_lakehouse.py).
retrieve():         hybrid search -> rerank -> top passages for the Agent.
"""
import os
import sys

sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "rag"))

from chunking import chunk_ticket
from vector_index import build_index
from hybrid_search import hybrid_search
from reranker import rerank


def index_gold_zone(gold_rows: list[dict]) -> None:
    """gold_rows: list of {ticket_id, document_text, ...} from the Gold Delta table."""
    all_chunks = []
    for row in gold_rows:
        all_chunks.extend(chunk_ticket(row["ticket_id"], row["document_text"]))
    build_index(all_chunks)


def retrieve(query: str, top_k_candidates: int = 10, top_k_final: int = 5) -> list[dict]:
    candidates = hybrid_search(query, top_k=top_k_candidates)
    return rerank(query, candidates, top_k=top_k_final)


if __name__ == "__main__":
    # Quick manual smoke test using the sample data directly (bypassing Spark)
    import json

    with open(os.path.join(os.path.dirname(__file__), "..", "..", "data", "sample_tickets.json")) as f:
        tickets = json.load(f)

    gold_rows = [
        {"ticket_id": t["ticket_id"], "document_text": f"{t['subject']}. {t['body']}"}
        for t in tickets
    ]
    index_gold_zone(gold_rows)

    results = retrieve("I never got my money back after cancelling")
    for r in results:
        print(f"{r['ticket_id']} | rerank={r.get('rerank_score', 0):.3f} | {r['text'][:80]}...")