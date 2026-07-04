"""
The single AI Agent required by the spec (note 1: "This project can be one
AI Agent only"). It cooperates with a free LLM (note 2: openai/gpt-oss-20b:free
via OpenRouter, or any other free model — just change OPENROUTER_MODEL).

The agent:
  1. Receives a user question.
  2. Calls the RAG pipeline (hybrid search + rerank) to retrieve grounding
     passages from real support tickets.
  3. Builds a grounded prompt and asks the free LLM to answer, citing the
     ticket_id(s) it used.
"""
import os
import sys

import requests
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), "rag"))
from rag_pipeline import retrieve  # noqa: E402

load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-oss-20b:free")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

SYSTEM_PROMPT = (
    "You are a support-ticket knowledge assistant. Answer using the "
    "provided context passages, even if the wording doesn't exactly match "
    "the question — use your judgment to connect relevant passages to the "
    "question. Only say you don't have enough information if the passages "
    "are truly unrelated to the topic. Always cite the ticket_id(s) "
    "you used in square brackets, e.g. [T-1002]."
)


def call_llm(prompt: str, max_tokens: int = 400) -> str:
    """Thin wrapper around the OpenRouter free-tier chat completion API."""
    response = requests.post(
        OPENROUTER_URL,
        headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}"},
        json={
            "model": OPENROUTER_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]


def build_context_block(passages: list[dict]) -> str:
    return "\n\n".join(f"[{p['ticket_id']}] {p['text']}" for p in passages)


def answer_question(question: str) -> dict:
    passages = retrieve(question)
    context = build_context_block(passages)

    prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        f"Context:\n{context}\n\n"
        f"Question: {question}\n"
        f"Answer:"
    )
    answer = call_llm(prompt)

    return {
        "question": question,
        "answer": answer,
        "sources": [p["ticket_id"] for p in passages],
        "passages_used": passages,
    }


if __name__ == "__main__":
    q = "A customer says they were charged twice, what should I check?"
    result = answer_question(q)
    print("Q:", result["question"])
    print("A:", result["answer"])
    print("Sources:", result["sources"])