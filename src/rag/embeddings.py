"""
Deliverable 3 (part of): Embeddings.

Uses a small, free, local sentence-transformers model (no API key, no
cost) to turn chunk text into dense vectors for the FAISS index.
"""
import os

from sentence_transformers import SentenceTransformer

MODEL_NAME = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

_model = None


def get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer(MODEL_NAME)
    return _model


def embed_texts(texts: list[str]):
    model = get_model()
    return model.encode(texts, normalize_embeddings=True, show_progress_bar=False)