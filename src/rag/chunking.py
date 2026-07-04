"""
Deliverable 3 (part of): Chunking strategy.

Support tickets are short, so we use sentence-aware chunking with overlap
rather than a naive fixed-character split, to avoid cutting a sentence (and
its meaning) in half — see the Day 3 "chunking strategies" material.
"""
import re


def split_sentences(text: str) -> list[str]:
    # Simple sentence splitter (good enough for short ticket text).
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    return [s for s in sentences if s]


def chunk_text(text: str, max_chars: int = 300, overlap_sentences: int = 1) -> list[str]:
    """Groups sentences into chunks under max_chars, carrying the last
    `overlap_sentences` sentences into the next chunk so context is never
    lost at a chunk boundary."""
    sentences = split_sentences(text)
    if not sentences:
        return []

    chunks, current = [], []
    current_len = 0

    for sentence in sentences:
        if current_len + len(sentence) > max_chars and current:
            chunks.append(" ".join(current))
            current = current[-overlap_sentences:] if overlap_sentences else []
            current_len = sum(len(s) for s in current)
        current.append(sentence)
        current_len += len(sentence)

    if current:
        chunks.append(" ".join(current))

    return chunks


def chunk_ticket(ticket_id: str, document_text: str) -> list[dict]:
    """Returns a list of {chunk_id, ticket_id, text} ready for embedding."""
    chunks = chunk_text(document_text)
    return [
        {"chunk_id": f"{ticket_id}-c{i}", "ticket_id": ticket_id, "text": chunk}
        for i, chunk in enumerate(chunks)
    ]