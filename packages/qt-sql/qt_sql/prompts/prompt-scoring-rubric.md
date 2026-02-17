# Prompt Quality Scoring Rubric

Use this rubric to evaluate any prompt on a 1–5 scale across eight dimensions. Total possible score: **40 points**.

---

## 1. Clarity & Specificity (1–5)

Does the prompt leave no room for misinterpretation?

| Score | Criteria |
|-------|----------|
| **5** | Every key term is defined. The request is unambiguous. A stranger could read it and know exactly what to produce. |
| **4** | Minor ambiguity exists but is unlikely to derail the output. |
| **3** | The intent is clear at a high level, but important details are left to assumption. |
| **2** | Vague language or conflicting instructions create confusion about what's wanted. |
| **1** | So unclear that the model must guess at the core intent. |

**Ask yourself:** If I handed this to a capable colleague with no other context, would they produce what I want on the first try?

---

## 2. Task Framing & Role Assignment (1–5)

Does the prompt establish *who* the model should be and *what kind of task* it's performing?

| Score | Criteria |
|-------|----------|
| **5** | A well-chosen role/persona is assigned and the task type is explicitly named (e.g., "Act as a senior data analyst. Write a root-cause analysis report…"). |
| **4** | Role or task type is stated; the other is reasonably implied. |
| **3** | No role is assigned but the task is clear enough to infer one. |
| **2** | The framing is generic or mismatched to the actual goal. |
| **1** | No framing at all; the model has no anchor for tone, expertise level, or task type. |

---

## 3. Context & Background (1–5)

Does the prompt supply enough context for the model to produce an informed response?

| Score | Criteria |
|-------|----------|
| **5** | All relevant background, constraints, audience, and domain-specific details are provided. Nothing critical is assumed. |
| **4** | Most context is present; one minor element could be added. |
| **3** | Some useful context is given, but significant assumptions must be made. |
| **2** | Very little context; the model is working largely blind. |
| **1** | No context at all—just a bare request. |

**Key context types to consider:** audience, domain, prior decisions, data sources, constraints, timeline, and purpose.

---

## 4. Output Specification (1–5)

Does the prompt define what the finished output should look like?

| Score | Criteria |
|-------|----------|
| **5** | Format, length, structure, tone, and any deliverable details are all specified (e.g., "Return a 3-column markdown table with headers: Feature, Pros, Cons. Keep each cell to ≤ 20 words."). |
| **4** | Most output parameters are specified; one is missing but easily inferred. |
| **3** | A general format is mentioned ("give me a list") but details like length or structure are absent. |
| **2** | Output expectations are vague ("explain this well"). |
| **1** | No output specification at all. |

---

## 5. Examples & Demonstrations (1–5)

Does the prompt include examples of desired (or undesired) outputs?

| Score | Criteria |
|-------|----------|
| **5** | Both positive and negative examples are provided, clearly illustrating the boundary between good and bad output. |
| **4** | At least one strong positive example is provided. |
| **3** | A brief or partial example is included. |
| **2** | An example is mentioned but is too vague or misaligned to be useful. |
| **1** | No examples provided, even though the task would clearly benefit from them. |

**Note:** Not every prompt needs examples. If the task is simple and unambiguous, score this dimension based on whether examples would have added value. For trivially clear tasks, a 3 is a reasonable baseline without examples.

---

## 6. Constraints & Guardrails (1–5)

Does the prompt define what the model should *avoid* or *not* do?

| Score | Criteria |
|-------|----------|
| **5** | Clear boundaries are set: topics to avoid, assumptions not to make, styles not to use, edge cases to handle, and failure modes to guard against. |
| **4** | Key constraints are present; minor edge cases are unaddressed. |
| **3** | One or two constraints are mentioned but others are missing. |
| **2** | Constraints are implied but not stated. |
| **1** | No constraints at all, despite the task having obvious pitfalls. |

**Common guardrails to consider:** "Don't hallucinate sources," "Don't use jargon the audience won't know," "If you're unsure, say so rather than guessing," "Do not include X."

---

## 7. Structure & Readability (1–5)

Is the prompt itself well-organized and easy to parse?

| Score | Criteria |
|-------|----------|
| **5** | Logical section ordering. Clear delimiters (headers, XML tags, numbered steps, or markdown). Long prompts use separators to distinguish instructions from data. White space is used intentionally. |
| **4** | Mostly well-structured with minor organizational issues. |
| **3** | Readable but could benefit from better formatting or ordering. |
| **2** | Dense wall of text with buried instructions. |
| **1** | Chaotic: instructions, data, and questions jumbled together. |

**Pro tip:** For prompts over ~200 words, use labeled sections (e.g., `## Role`, `## Task`, `## Constraints`, `## Output Format`) or XML tags (e.g., `<instructions>`, `<context>`, `<examples>`).

---

## 8. Reasoning & Chain-of-Thought Guidance (1–5)

Does the prompt guide the model's *thinking process*, not just its output?

| Score | Criteria |
|-------|----------|
| **5** | The prompt explicitly instructs the model to reason step-by-step, consider alternatives, show its work, or think before answering. Intermediate steps are requested where they add value. |
| **4** | Some reasoning guidance is present (e.g., "explain your reasoning"). |
| **3** | The prompt implicitly encourages reasoning through multi-part questions. |
| **2** | No reasoning guidance, though the task clearly benefits from structured thinking. |
| **1** | The prompt actively discourages reasoning (e.g., demands a snap answer on a complex topic) or the task requires deep analysis but no thinking structure is provided. |

**Note:** Simple factual lookups don't need chain-of-thought. Score this relative to the complexity of the task.

---

## Scoring Summary

| Dimension | Score (1–5) |
|-----------|-------------|
| 1. Clarity & Specificity | |
| 2. Task Framing & Role Assignment | |
| 3. Context & Background | |
| 4. Output Specification | |
| 5. Examples & Demonstrations | |
| 6. Constraints & Guardrails | |
| 7. Structure & Readability | |
| 8. Reasoning & Chain-of-Thought Guidance | |
| **Total** | **/ 40** |

### Score Interpretation

| Range | Rating | Meaning |
|-------|--------|---------|
| 35–40 | ★★★★★ Excellent | Production-ready prompt. Expect high-quality, consistent outputs. |
| 28–34 | ★★★★ Good | Solid prompt. Minor refinements would push it to excellent. |
| 20–27 | ★★★ Adequate | Functional but will produce inconsistent results. Address the lowest-scoring dimensions first. |
| 12–19 | ★★ Weak | Significant gaps. The model is doing a lot of guesswork. |
| 8–11 | ★ Poor | Needs a fundamental rewrite before use. |

---

## Quick-Reference Checklist

Before submitting a prompt, verify:

- [ ] Could someone unfamiliar with my goal understand exactly what I want?
- [ ] Have I told the model *who* to be and *what kind of task* this is?
- [ ] Have I provided all the background the model needs?
- [ ] Have I described the desired output format, length, and structure?
- [ ] Have I included at least one example (if the task is non-trivial)?
- [ ] Have I stated what to avoid or watch out for?
- [ ] Is the prompt itself cleanly formatted and easy to scan?
- [ ] For complex tasks, have I asked the model to think step-by-step?
