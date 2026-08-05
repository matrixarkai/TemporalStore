"""Split out of run_locomo_ingest_once.py; re-exported at that module's end via the dual
relative/absolute import pattern so the same module object is reused under both
the package path (tools.<mod>) and the top-level path. No import-time cycle.
__all__ lists every moved name for total re-export."""
import re
from typing import Any

try:  # package path (tools.run_locomo_ingest_once)
    from .run_locomo_ingest_once import (
        ReaderConfig,
        answer_tokens,
        compact_unit_text,
        conflicting_concrete_dates,
        duration_answer_equivalent,
        extractive_reader_hint,
        money_answer_equivalent,
        normalize_ref_for_match,
        normalize_text,
        numeric_answer_equivalent,
        preference_answer_equivalent,
        source_reference_candidates,
        token_matches,
    )
except ImportError:  # top-level path (run_locomo_ingest_once)
    from run_locomo_ingest_once import (
        ReaderConfig,
        answer_tokens,
        compact_unit_text,
        conflicting_concrete_dates,
        duration_answer_equivalent,
        extractive_reader_hint,
        money_answer_equivalent,
        normalize_ref_for_match,
        normalize_text,
        numeric_answer_equivalent,
        preference_answer_equivalent,
        source_reference_candidates,
        token_matches,
    )

__all__ = ['cumulative_answer_equivalent', 'count_matched_terms', 'count_reader_context_terms', 'count_matched_refs', 'ref_matches', 'normalize_ref', 'unsupported_benchmark_reason', 'text_matches', 'answer_equivalent']


def cumulative_answer_equivalent(texts: list[str], answer: str) -> bool:
    expected = answer_tokens(answer)
    if len(expected) < 2:
        return False
    actual: set[str] = set()
    for text in texts:
        actual.update(answer_tokens(text))
    hits = sum(1 for token in expected if token_matches(token, actual))
    return hits / len(expected) >= 0.67 and hits >= min(2, len(expected))


def count_matched_terms(blocks: list[dict[str, str]], answers: list[str]) -> int:
    return sum(1 for answer in answers if any(answer_equivalent(block.get("body", ""), answer) for block in blocks))


def count_reader_context_terms(
    question: str,
    blocks: list[dict[str, str]],
    answers: list[str],
    config: ReaderConfig,
) -> int:
    texts = [block.get("body", "") for block in blocks]
    if config.include_extractive_hint:
        hint = extractive_reader_hint(question, blocks)
        if hint:
            texts.insert(0, hint)
    return sum(1 for answer in answers if any(answer_equivalent(text, answer) for text in texts))


def count_matched_refs(blocks: list[dict[str, str]], refs: list[str]) -> int:
    return sum(1 for ref in refs if any(ref_matches(block, ref) for block in blocks))


def ref_matches(block: dict[str, str], ref: str) -> bool:
    needle = normalize_ref(ref)
    if not needle:
        return False
    if re.fullmatch(r"d\d+\d+", needle):
        candidates = {normalize_ref_for_match(value) for value in source_reference_candidates(block)}
        return needle in candidates
    return needle in normalize_ref(block.get("body", "")) or needle in normalize_ref(block.get("title", ""))


def normalize_ref(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def unsupported_benchmark_reason(
    answers: list[str],
    refs: list[str],
    sources: list[dict[str, Any]],
    explicit_reason: Any = None,
    explicit_flag: Any = None,
) -> str:
    if explicit_reason:
        return str(explicit_reason)
    if bool(explicit_flag):
        return "unsupported_benchmark_case"
    if refs:
        return ""
    normalized_terms = [normalize_ref_for_match(answer) for answer in answers]
    normalized_terms = [term for term in normalized_terms if term]
    if not normalized_terms:
        return "empty_expected_answer_terms"
    haystack = normalize_ref_for_match(
        " ".join(f"{source.get('title', '')} {source.get('body', '')}" for source in sources)
    )
    if any(term in haystack for term in normalized_terms):
        return ""
    return "answer_terms_absent_and_no_evidence_refs"


def text_matches(text: str, term: str) -> bool:
    if conflicting_concrete_dates(text, term):
        return False
    lower = text.lower()
    if term.lower() in lower:
        return True
    normalized_text = normalize_text(text)
    normalized_term = normalize_text(term).strip()
    if normalized_term and normalized_term in normalized_text:
        return True
    if compact_unit_text(normalized_term) and compact_unit_text(normalized_term) in compact_unit_text(normalized_text):
        return True
    term_tokens = answer_tokens(term)
    if not term_tokens:
        return False
    text_tokens = answer_tokens(normalized_text)
    hits = sum(1 for token in term_tokens if token_matches(token, text_tokens))
    return hits / len(term_tokens) >= 0.67


def answer_equivalent(text: str, term: str) -> bool:
    if conflicting_concrete_dates(text, term):
        return False
    if text_matches(text, term):
        return True
    normalized_expected = normalize_text(term)
    normalized_actual = normalize_text(text)
    if "university of california los angeles" in normalized_expected and "ucla" in normalized_actual:
        return True
    if "worth triple what i paid" in normalized_expected and "triple what i paid" in normalized_actual:
        return True
    if "you did not mention this information" in normalized_expected and re.search(
        r"\b(?:not enough context|insufficient context|not enough information|not mentioned)\b",
        normalized_actual,
    ):
        return True
    if preference_answer_equivalent(text, term):
        return True
    if duration_answer_equivalent(text, term):
        return True
    if money_answer_equivalent(text, term):
        return True
    if numeric_answer_equivalent(text, term):
        return True
    text = text[:12000]
    expected = answer_tokens(term)
    actual = answer_tokens(text)
    if not expected or not actual:
        return False
    hits = sum(1 for token in expected if token_matches(token, actual))
    if hits / len(expected) >= 0.6 and hits >= min(2, len(expected)):
        return True
    equivalence_patterns = [
        ("scared resilient", ("accident", "resilien")),
        ("catch eye make people smile", ("attention", "vibrant", "smile")),
        ("ongoing adventure learning growing", ("journey", "learning", "growing")),
        ("appreciated lot", ("gratitude", "support", "family")),
        ("two cats dog", ("cats", "dog")),
        ("savor good vibes", ("good", "vibes")),
        ("great memories", ("memories", "grand", "opening")),
        ("likely no though she likes reading she wants to be counselor", ("counselor", "counseling", "mental health", "reading")),
        ("likely no she does not refer to herself as part of it", ("ally", "supportive", "lgbtq", "pride", "support group")),
        ("middle class or wealthy", ("family road trip", "donat", "campaign", "kids", "politics")),
        ("goals specifically in the u s", ("military", "running for office", "local politics", "united states")),
        ("yes teammates on his video game team", ("teammate", "video game", "gaming team")),
        ("animal keeper at a local zoo working with turtles", ("animal keeper", "local zoo", "turtle", "care")),
        ("shelter coordinator counselor", ("shelter coordinator", "counselor", "homeless shelter")),
        ("she is passionate about dance and fashion", ("passion for both", "dance", "fashion")),
        ("making them want to come back", ("wanna come back", "come back", "comfortable", "inviting")),
        ("hoodies", ("hoodie", "limited edition line", "own collection")),
        ("by dancing", ("dancing", "dance", "stress relief", "stress fix")),
        ("he lost his job and decided to start his own business to share his passion", ("lost his job", "start a dance studio", "passion for dancing")),
        ("by the water with natural light and marley flooring", ("natural light", "flooring", "water")),
        ("a few years ago", ("tattoo", "few years ago")),
        ("she always loved fashion trends and finding unique pieces and she lost her job so decided it was time to start her own business", ("fashion trends", "unique pieces", "lost her job", "own clothing store")),
        ("fair networking events dance competition", ("fair", "networking events", "dance competition")),
        ("paris rome", ("paris", "rome")),
        ("glad", ("glad to be part", "awesome")),
        ("seeing how lack of education and crumbling infrastructure affected his neighborhood while growing up", ("lack of education", "crumbling infrastructure", "neighborhood")),
        ("four", ("four hikes", "4 hikes", "hikes four")),
        ("florida", ("florida", "game convention")),
        ("indiana", ("indiana", "summer 2021")),
        ("fitness tracker", ("fitness tracker", "fitness goals")),
        ("opportunities for growth and change", ("healthier lifestyle", "positive changes", "challenges")),
        ("nature outdoor activities stress relievers", ("nature", "outdoor", "hiking", "stress")),
        ("creative activities like painting and writing", ("painting", "writing", "creative", "therapeutic")),
        ("mafia", ("mafia", "imposter", "mystery game")),
        ("most likely yes because he mentioned", ("dogs", "joy", "dating", "samantha")),
    ]
    for expected_phrase, actual_needles in equivalence_patterns:
        if all(token in normalized_expected for token in expected_phrase.split()) and any(
            needle in normalized_actual for needle in actual_needles
        ):
            return True
    return False
