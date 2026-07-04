"""
Deliverable 3 (part of): Vector index.

FAISS is used as a free, local, dependency-light vector store — swap for
Qdrant/Milvus in a real production deployment without changing the rest of
the RAG pipeline (the interface below stays the same).
"""
import json
import os

import faiss
import numpy as np

from embeddings import embed_texts

INDEX_DIR = os.getenv("VECTOR_INDEX_PATH", "./lakehouse/gold/vector_index")
INDEX_FILE = os.path.join(INDEX_DIR, "index.faiss")
METADATA_FILE = os.path.join(INDEX_DIR, "metadata.json")


def build_index(chunks: list[dict]) -> None:
    """chunks: list of {chunk_id, ticket_id, text}"""
    os.makedirs(INDEX_DIR, exist_ok=True)
    texts = [c["text"] for c in chunks]
    vectors = np.array(embed_texts(texts)).astype("float32")

    dim = vectors.shape[1]
    index = faiss.IndexFlatIP(dim)  # cosine similarity via normalized vectors
    index.add(vectors)

    faiss.write_index(index, INDEX_FILE)
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)

    print(f"[vector_index] indexed {len(chunks)} chunks -> {INDEX_FILE}")


def load_index():
    index = faiss.read_index(INDEX_FILE)
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        metadata = json.load(f)
    return index, metadata


def vector_search(query: str, top_k: int = 10) -> list[dict]:
    index, metadata = load_index()
    query_vec = np.array(embed_texts([query])).astype("float32")
    scores, indices = index.search(query_vec, top_k)

    results = []
    for score, idx in zip(scores[0], indices[0]):
        if idx == -1:
            continue
        chunk = dict(metadata[idx])
        chunk["vector_score"] = float(score)
        results.append(chunk)
    return results