# Automated classifiers for threatening/malicious Instagram comments (as of Nov 2026)

Scope: options for sorting short, slangy, often sarcastic English (US) Instagram comments (for example motorists on a cycling creator's posts: "I'd run him over", "next time I'll hit you", "roll coal on these idiots") into benign / rude-insulting / harassment / generalized violent rhetoric / specific threat against an identifiable person, with output a human or legal reviewer can use.

Note on sourcing: research ran 2026-11-22. Claude model prices come from Anthropic's model/pricing table as cached on 2026-11-03 (bundled Claude API reference), plus launch coverage for Haiku 5.5. Check them at https://platform.claude.com/docs/en/about-claude/pricing before budgeting. Several vendor prices come from third parties and are marked as such.

## 1. Perspective API (Google Jigsaw): status, THREAT attribute, quotas, cost

### Takeaway
Perspective is shutting down. The official site says it stays active until December 31, 2026, stopped taking new usage/quota requests in February 2026, and comes with no migration support. A new project started in late 2026 should not use it. At best it is a short-lived baseline for anyone who already has a key.

### Cited Findings
- The official site says the service "will remain active until December 31, 2026" and "will no longer be in service after 2026." — [Perspective API](https://perspectiveapi.com/)
- Usage/quota requests were accepted only until February 2026, with none taken after that. — [Perspective API](https://perspectiveapi.com/)
- "We will not be offering direct migration support." — [Perspective API](https://perspectiveapi.com/)
- A vendor writeup (Stream, which sells a competing product, so it is biased) confirms the timeline. It lists Perspective's price as "Free," maps Perspective's THREAT attribute to Stream's "Threat" label, and says Perspective was "easy to evade" with obfuscation (extra spaces, symbols, lookalike characters). — [GetStream blog](https://getstream.io/blog/perspective-api-alternative/)
- Hindsight has publicly offered up to $1M toward alternatives in response to the shutdown. — [The Next Web](https://thenextweb.com/news/hindsight-ai-content-moderation-perspective-api-shutdown)
- A Spanish-language report suggests integrations could fail from January 1, 2027. This is the outlet's inference, not an official statement. — [DiarioBitcoin](https://www.diariobitcoin.com/tecnologia/hindsight-ofrece-hasta-usd-1-millon-mientras-google-cierra-perspective-api/)
- In Kumar et al., GPT models reached 0.71–0.73 accuracy on toxic-content detection, against a maximum of 0.66 for Perspective TOXICITY at a 0.5 threshold. LLMs outperformed Perspective "across almost all metrics." — [Watch Your Language, arXiv 2309.14517](https://arxiv.org/pdf/2309.14517)
- Another study found Perspective's default threshold "too stringent." With a laxer threshold it was competitive with GPT-4o. — [arXiv 2409.09741](https://arxiv.org/pdf/2409.09741)

### Inferences
- A Perspective-based pipeline would need replacing within about 5 weeks (by 2026-12-31). Archival scores from it would also not be reproducible later, which weakens it as evidence for a legal file.
- Even while it works, THREAT is a single generic probability. It does not separate "generalized violent rhetoric" from "specific threat to an identifiable person," and that distinction is the one this project needs most.

### Gaps
- I could not retrieve a THREAT-specific accuracy/AUC figure or the documented QPS quota. The developer attribute page returned a CSS error page. From memory, the historical default quota was about 1 QPS (free), but this is unverified.
- No independent evaluation of THREAT on hyperbolic road-rage-style comments was found.

## 2. OpenAI Moderation API (omni-moderation-latest)

### Takeaway
The endpoint is free, has no announced deprecation, and returns per-category 0–1 scores. The useful categories here are `harassment`, `harassment/threatening`, `violence`, and `violence/graphic`. It is a good cheap first-pass signal. OpenAI itself says to treat the scores as signals rather than decisions, and its taxonomy does not separate a specific threat from generalized violent rhetoric.

### Cited Findings
- Relevant categories (text): `harassment` ("expresses, incites, or promotes harassment toward any target"); `harassment/threatening` ("harassment that also includes violence or serious harm toward any target"); `hate`, `hate/threatening`; `illicit`, `illicit/violent`; `violence` ("depicts death, violence, or physical injury"); `violence/graphic`; plus self-harm and sexual categories. `harassment/threatening` is text-only. — [OpenAI Moderation guide](https://developers.openai.com/api/docs/guides/moderation)
- The output includes `flagged`, per-category booleans, `category_scores` (0–1), and `category_applied_input_types`. — [OpenAI Moderation guide](https://developers.openai.com/api/docs/guides/moderation)
- "The moderation endpoint is free to use." — [OpenAI Moderation guide](https://developers.openai.com/api/docs/guides/moderation); usage doesn't count toward monthly usage limits — [OpenAI Help Center](https://help.openai.com/ro-ro/articles/4936833-is-the-moderation-endpoint-free-to-use). Rate limits depend on usage tier — [OpenAI model page](https://developers.openai.com/api/docs/models/omni-moderation-latest)
- OpenAI gives no recommended thresholds. It says to treat scores as signals for your own policy, and that the model is "continuously upgraded," so custom thresholds "may need recalibration over time." — [OpenAI Moderation guide](https://developers.openai.com/api/docs/guides/moderation)
- OpenAI's omni-moderation launch post describes multimodal (text and image) support. — [OpenAI blog](https://openai.com/index/upgrading-the-moderation-api-with-our-new-multimodal-moderation-model/)

### Inferences
- Because `-latest` is silently upgraded, scores are not stable over time. For a legal-review audit trail, log the model version string returned in each response, the raw scores, and the date.
- "I'd run him over" will likely score high on `violence` and `harassment/threatening` whether it's a joke or a real threat. The API cannot use context such as the commenter knowing where the creator rides. It works best as a high-recall prefilter in front of an LLM or human stage.
- Usage terms: the endpoint is intended for moderating content. Using it on third-party comments a creator receives looks consistent with that purpose, but I did not verify the specific ToS wording.

### Gaps
- No published precision/recall for `harassment/threatening` on short social-media threats was found. The developer PDF ([omni moderation info](https://cdn.openai.com/API/docs/omni_moderation_information_for_developers.pdf)) was not fetched.

## 3. LLM-as-judge with Claude (or other frontier models): rubric, cost, batch, policy

### Takeaway
An LLM with a written rubric and JSON-schema structured output can produce exactly the five-way label plus a rationale, target identification, and conditional/hyperbole flags. That gives a downstream reviewer something to work with. At current Claude prices this costs well under $1 per 1,000 comments on Haiku 5.5 and a few dollars per 1,000 on Sonnet 5.5. The Batch API halves those prices. Research consistently finds LLMs beat Perspective-style classifiers, though by modest margins that depend on the dataset.

### Cited Findings
- Current Anthropic list prices per 1M tokens (input/output), as cached 2026-11-03:
  - Claude Haiku 5.5: $0.10 / $0.50 for prompts up to 100K tokens ($0.50 / $2.50 beyond)
  - Claude Sonnet 5.5: $2 / $10
  - Claude Opus 5.5: $4 / $20
  - Claude Haiku 4.5 (previous generation): $1 / $5

  [Anthropic pricing](https://platform.claude.com/docs/en/about-claude/pricing). The Haiku 5.5 launch prices ($0.10/$0.50, down from $1/$5 for Haiku 4.5) are corroborated by [TechRepublic](https://www.techrepublic.com/article/news-anthropic-claude-haiku-5-5-api-pricing/) and [VentureBeat](https://venturebeat.com/ai/anthropic-launches-claude-haiku-5-5-with-90-api-price-reduction-matching-gpt-6-luna/).
- Batch processing: "All usage is charged at 50% of the standard API prices." Batches are asynchronous. — [Anthropic batch processing docs](https://platform.claude.com/docs/en/build-with-claude/batch-processing)
- Structured outputs (`output_config.format` with a JSON schema) constrain the response to a schema. Prompt caching cuts the cost of a repeated system prompt/rubric: cache reads cost about 0.1× the input price, but the minimum cacheable prefix is model-dependent (512–4096 tokens), so a short rubric may not cache. Source: Anthropic API reference (bundled Claude API skill, cached 2026-11-03); see [Anthropic docs](https://platform.claude.com/docs/en/build-with-claude/batch-processing).
- Current Claude models can end a response with `stop_reason: "refusal"` from safety classifiers. Claude Sonnet 5.5's refusal categories include `general_harms`. A pipeline must check `stop_reason` and route refusals to human review rather than dropping them. Source: Anthropic API reference (bundled skill, cached 2026-11-03).
- Anthropic's own content-moderation guide recommends using Claude as a moderation filter. It notes a moderation system must recognize that "killed it" is a metaphor, not actual violence. — [Anthropic content moderation guide](https://docs.anthropic.com/en/docs/about-claude/use-case-guides/content-moderation)
- Anthropic's Usage Policy bars *generating* violent or gory content inspired by real acts of violence. It does not prohibit *classifying* violent content. The policy is versioned, with an effective date of November 12, 2026 on the current snippet. — [Anthropic Usage Policy](https://www.anthropic.com/legal/aup)
- Kumar et al.: GPT-family LLMs hit 0.71–0.73 accuracy vs 0.66 for Perspective. Larger models did not always help, and models often missed implicit toxicity. — [arXiv 2309.14517](https://arxiv.org/pdf/2309.14517)
- Prior work cited in a 2026 Bluesky study found proprietary LLMs comparable to human moderators on rule-based moderation and significantly better than Perspective. — [arXiv 2602.05189](https://arxiv.org/html/2602.05189)
- I found no published head-to-head results for Claude on threat detection (see Gaps).

### Worked cost estimates (my arithmetic from the list prices above, not sourced figures)
Assumptions: rubric/system prompt of about 1,500 tokens, comment plus context features of about 150 tokens, and JSON output of about 150 tokens, so about 1,650 input and 150 output tokens per comment, one comment per request, no caching. Thinking tokens bill as output. Lower effort and short outputs keep them small, but they can add materially, especially on Sonnet and Opus.

| Model | Standard $/1,000 comments | Batch (50%) $/1,000 |
|---|---|---|
| Haiku 5.5 | ≈ $0.24 | ≈ $0.12 |
| Haiku 4.5 | ≈ $2.40 | ≈ $1.20 |
| Sonnet 5.5 | ≈ $4.80 (+≈$3 per 300 thinking tokens per comment) | ≈ $2.40 |
| Opus 5.5 | ≈ $9.60 | ≈ $4.80 |

- Batching several comments per request, or caching the rubric when it is long enough to cache, lowers these further.
- Tiered design: run Haiku 5.5 (or the free OpenAI Moderation endpoint) on everything, then send only comments flagged "harassment or worse" to Sonnet/Opus. On a typical comment stream where most comments are benign, the total stays around $1 per 1,000 or less.

### Inferences
- Suggested output schema: `label` (enum of the five classes); `target` (creator / identifiable third party / group / none); `explicit_intent` (bool); `conditional` (e.g. "if I see you…"); `hyperbole_or_joke_markers` (e.g. "lol", emoji); `references_location_or_schedule` (bool, with quoted span); `vehicle_or_weapon_reference`; `confidence`; `rationale` (1–2 sentences quoting the span). A structured rationale like this is what makes the output usable for a human or legal reviewer.
- For legal review, keep the model ID, prompt version, and timestamp for every decision, and treat the LLM label as triage, never as a determination of a "true threat," which is a legal standard (e.g. US Counterman v. Colorado, 2023, recklessness standard; not researched here).
- Other frontier models (OpenAI, Google Gemini) could fill the same role. I did not research their current pricing in this pass.

### Gaps
- No public benchmark of Claude 5.x-generation models on threat detection specifically.
- No measured refusal rate for Claude when classifying (not generating) violent threats. Pilot testing is needed.

## 4. Commercial T&S vendors and open-source guard models: pricing and suitability

### Takeaway
Off-the-shelf vendors (Hive, Azure AI Content Safety, AWS Comprehend) are cheap, from about $0.10 to $0.50 per 1,000 short comments, and fast. But their taxonomies are generic (violence / harassment / insult), so none of them natively separates a specific threat against an identifiable person from violent rhetoric. Enterprise platforms (ActiveFence, which absorbed Spectrum Labs; Checkstep) price through sales and suit platforms better than a single creator. Open-weight guard models (Llama Guard 4, ShieldGemma) are free to self-host, but they are built for LLM prompt/response safety and a 2026 benchmark found them conservative, missing many unsafe items.

### Cited Findings
**Hive Moderation**
- Text moderation costs $0.50 per 1,000 requests on the pay-as-you-go Developer tier, with $50 of free credits. The Developer tier is limited to 100 requests/day for testing. Production volumes require the Enterprise tier at custom pricing. — [Hive pricing](https://thehive.ai/pricing); [Hive text moderation docs](https://docs.thehive.ai/docs/classification-text)

**Azure AI Content Safety**
- Four harm categories: Hate, Sexual, Violence, Self-harm. Violence explicitly includes "bullying and intimidation" and "stalking." Text severity runs on a 0–7 scale, or a trimmed 0/2/4/6. Level 4 includes "threats of violent acts and fantasies relating to how to harm another person." Level 5 includes "texts that incite physical violence against targeted individuals/groups" and "bullying and intimidation." Level 3 example: "Some people just need to be slapped." — [Microsoft Learn: harm categories](https://learn.microsoft.com/en-us/azure/ai-services/content-safety/concepts/harm-categories) (page updated 2026-09-18)
- English is among the trained-and-tested languages. Microsoft advises evaluating false positives and negatives on representative content. — [Microsoft Learn](https://learn.microsoft.com/en-us/azure/ai-services/content-safety/concepts/harm-categories)
- Billing is per text record of up to 1,000 characters, with a free tier of 5,000 records/month. — [Azure pricing](https://azure.microsoft.com/en-us/pricing/details/content-safety/). The official page showed the S-tier rate as a placeholder. A third-party guide lists $0.38 per 1,000 text records, which is unverified. — [Turbo360](https://azure-cost-management-playbook.turbo360.com/docs/ai-content-safety)

**AWS Comprehend toxicity detection**
- Returns an overall toxicity score (0–1) plus labels including PROFANITY, HATE_SPEECH, INSULT, GRAPHIC, HARASSMENT_OR_ABUSE, SEXUAL, and VIOLENCE_OR_THREAT. English-only. — [AWS docs: trust and safety](https://docs.aws.amazon.com/comprehend/latest/dg/trust-safety.html); [AWS blog](https://aws.amazon.com/blogs/aws/new-for-amazon-comprehend-toxicity-detection)
- Pricing is said to follow standard NLP units: $0.0001 per 100-character unit for the first 10M units, 3-unit minimum per request. This is a third-party figure. — [CloudZero](https://www.cloudzero.com/blog/amazon-comprehend-pricing/); official page: [AWS Comprehend pricing](https://aws.amazon.com/comprehend/pricing/)

**ActiveFence / Spectrum Labs**
- ActiveFence acquired Spectrum Labs, a contextual-AI moderation vendor, on 2023-09-07. Spectrum's models were folded into ActiveFence's ActiveOS platform. Spectrum's last reported valuation was $137M; the deal price was not disclosed. — [TechCrunch](https://techcrunch.com/2023/09/07/activefence-snaps-up-spectrum-labs-last-valued-at-137m-to-help-fight-the-harmful-content-creep/amp); [ActiveFence announcement](https://www.activefence.com/?p=6392)
- A separate press page says Alice acquired SpectrumLabs. This may be an ActiveFence rebrand; I did not verify it. — [alice.io](https://alice.io/company-news/alice-acquires-spectrumlabs)

**Checkstep**
- I found no public pricing.

**Llama Guard 4 / open models**
- Llama Guard 4 is a 12B natively multimodal classifier using the MLCommons hazard taxonomy (S1 Violent Crimes, S2 Non-Violent Crimes, hate, etc.). — [Meta Llama Guard 4 model card](https://developer.meta.com/ai/docs/model-cards-and-prompt-formats/llama-guard-4/); [NVIDIA NIM model card](https://build.nvidia.com/meta/llama-guard-4-12b/modelcard)
- A 2026 arXiv benchmark of 14 open guard models (79,331 samples) found larger models such as Llama Guard (12B) and GPT-OSS Safeguard (20B) "conservative," missing up to 75% of unsafe content. — [arXiv 2605.28830](https://arxiv.org/pdf/2605.28830)
- ShieldGemma 2 is an image-safety model (Gemma 3 4B). The text ShieldGemma models' categories were not retrieved. — [ShieldGemma model card](https://ai.google.dev/gemma/docs/shieldgemma/model_card_2.md.txt)

### Inferences
- Of the cheap APIs, AWS Comprehend's VIOLENCE_OR_THREAT label and Azure's severity rubric (threat = level 4–5) come closest to a threat signal. Neither identifies the target, which is needed for the "identifiable person" class.
- Open guard models are tuned for "is this LLM prompt/response unsafe," not for social-media threat triage. They add hosting overhead and offer no clear accuracy advantage for this use case.
- For one creator's comment volume (likely thousands, not millions, per month), cost barely matters across options. Quality on hyperbole and target identification should decide the choice, which favors an LLM rubric stage.

### Gaps
- Checkstep and ActiveFence pricing are not public (sales-quoted). Whether the "Alice" brand replaced ActiveFence was not confirmed.
- I found no vendor-independent accuracy comparison of Hive, Azure, and AWS on threats.

## 5. Accuracy evidence: sarcasm, quoting, hyperbole, dialect bias, threat benchmarks, thresholds, human-in-the-loop

### Takeaway
Keyword- and lexicon-driven toxicity classifiers produce many false positives on profanity, emotional language, and quoted toxicity, and show documented dialect (African American English) bias. Threat-labeled training data is scarce. Hyperbole ("I could kill him lol") is a known weak spot, and no specific benchmark measures it. Thresholds should be set on a locally labeled sample, with every "specific threat" output going to human review.

### Cited Findings
- Sap et al. (ACL 2019) found correlations between African American English markers and toxicity labels in widely used datasets. AAE tweets were up to 2× more likely to be labeled offensive. Priming annotators with dialect reduced this. — [ACL Anthology P19-1163](https://aclanthology.org/P19-1163/)
- A UW news piece reports 46% of non-offensive AAE tweets were flagged offensive vs 9% for general American English, in work that examined Perspective. Whether that figure applies to Perspective specifically is not verified. — [UW Allen School news](https://news.cs.washington.edu/2019/10/09/allen-school-researchers-find-racial-bias-built-into-hate-speech-detection/)
- Davidson et al. (2019) found systematic racial bias in all abusive-language datasets they examined. — [arXiv 1905.12516](https://arxiv.org/pdf/1905.12516)
- A 2026 paper argues "Harm is behavioural, moderation is lexical": automated systems key on words, not behavior. — [arXiv 2610.31047](https://arxiv.org/pdf/2610.31047)
- Risch & Krestel describe false positives from non-toxic comments that quote toxic ones, and stress that context includes the surrounding comments. — [Risch & Krestel 2019](https://hpi.de/oldsite/fileadmin/user_upload/fachgebiete/naumann/publications/2019/risch2019toxic.pdf)
- A study of a NZ political-tweet bot found curse words triggered high toxicity despite neutral or positive intent. Manual review found 40% false positives among sampled flagged tweets. — [arXiv 2110.00116](https://arxiv.org/pdf/2110.00116)
- 2026 explainability study: false positives were emotionally expressive or politically affiliated comments with no harassment. False negatives were indirect, rhetorical, or context-dependent toxicity. — [arXiv 2603.18015](https://arxiv.org/pdf/2603.18015)
- Threat data scarcity: in Jigsaw-style data, 15,000+ comments are labeled toxic but fewer than 500 are labeled threat, and some labels are inaccurate. This is a student report, so lower quality. — [U of Toronto ECE324 report](https://www.eecg.utoronto.ca/~jayar/ece324/2020/download/toxiccommentclassifier.pdf)
- THREAT corpus (Hammer et al., 2019): about 30,000 sentences from about 10,000 YouTube comments, each manually labeled as violent threat or not. It is described as the first public dataset of its kind. — [OsloMet ODA](https://oda.oslomet.no/oda-xmlui/handle/10642/8506)
- InViS (Chen et al., WebSci '25): about 30,000 comments from Reddit, Parler, and incels.is labeled for violence. Cross-platform transfer with the YouTube THREAT data worked well. — [arXiv 2506.03312](https://arxiv.org/pdf/2506.03312)
- Anthropic's moderation guide notes metaphors like "killed it" must not be treated as violence. — [Anthropic docs](https://docs.anthropic.com/en/docs/about-claude/use-case-guides/content-moderation)
- OpenAI says thresholds are policy choices and need recalibration as the model changes. — [OpenAI Moderation guide](https://developers.openai.com/api/docs/guides/moderation). Perspective's default 0.5 threshold proved too stringent in one study. — [arXiv 2409.09741](https://arxiv.org/pdf/2409.09741)

### Inferences
- Threshold recommendation (my synthesis; no vendor gives a numeric threshold for this use):
  1. Hand-label 200–500 real comments from the creator's account across the five classes.
  2. Pick the prefilter cutoff for very high recall on threat/harassment (accept many false positives).
  3. Put the LLM rubric stage after the prefilter.
  4. Send every "specific threat" and every low-confidence "violent rhetoric" item to human review.
  5. Re-check the thresholds whenever the vendor model changes.
- Ask the LLM to label hyperbole and joke markers ("lol", emoji, "jk") separately rather than discounting them. Road-rage threats are often phrased as jokes. A reviewer should see the joke marker and the threat together, not a suppressed label.
- Cycling-specific slang ("roll coal," "brake check," "buzz them," "doored") will be underweighted by generic classifiers. Put definitions in the rubric.
- Dialect bias argues against auto-actioning on a classifier score alone. Human review should own any decision with consequences.

### Gaps
- No benchmark specifically on hyperbolic or idiomatic violent statements ("I'd run him over") vs genuine threats was found.
- No study on threats against cyclists or road-rage language online was found.
- No published threshold recommendation for threat triage feeding legal review.

## 6. Adding context features (location references, repeat commenters, proximity)

### Takeaway
No vendor API takes context features like these natively. They are best added as engineered features alongside the LLM prompt or as a separate rules/risk layer. Examples: whether the comment mentions the creator's route, town, or schedule; how many prior hostile comments the same account has posted; whether the account appears local. Research supports conversational context as an accuracy lever. Threat-assessment escalation logic (specificity, repetition, proximity, means) has to be designed in, not bought.

### Cited Findings
- Comment context, meaning other comments in the discussion, matters for correct toxicity judgments. — [Risch & Krestel 2019](https://hpi.de/oldsite/fileadmin/user_upload/fachgebiete/naumann/publications/2019/risch2019toxic.pdf)
- The CAALDYC dataset adds reply context to YouTube abusive-language detection on the premise that context improves classification. — [PMC: Ashraf et al. 2021](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC8507480/)
- A 2026 study evaluated "context-aware scoring" vs supervised classification for social-media toxicity. Full results were not retrieved. — [ResearchGate](https://www.researchgate.net/publication/404670688_Evaluating_Context-Aware_Scoring_and_Supervised_Classification_Methods_for_Toxicity_Detection_in_Social_Media_Comments)
- Azure's violence rubric treats "stalking" and "illegal harassing behaviors such as stalking or unwanted surveillance" as top-severity (level 7). This suggests that location or surveillance references raise severity. — [Microsoft Learn](https://learn.microsoft.com/en-us/azure/ai-services/content-safety/concepts/harm-categories)
- False negatives cluster in "indirect, rhetorical, or context-dependent" toxicity. — [arXiv 2603.18015](https://arxiv.org/pdf/2603.18015)

### Inferences
- Practical design (my synthesis):
  1. Per comment, pass the LLM the comment, the post caption, the parent comment if it's a reply, and a short "commenter history" block: count and labels of prior comments on this creator, and first-seen date.
  2. Run a gazetteer/regex step for the creator's known locations, routes, landmarks, and times, and flag matches before the LLM call, so the LLM gets `mentions_creator_location: true` plus the quoted span.
  3. Compute escalation features outside the model: repeat-offender count, increasing severity over time, cross-post following, a direct "you" address vs third person, mentions of a vehicle as a weapon.
  4. Build an aggregate per-account risk score for the reviewer queue, so repeated "violent rhetoric" from one account can surface as a pattern even when no single comment is a "specific threat."
- "Apparent proximity" (e.g., "I see you on X Road every Tuesday") is best captured with an LLM extraction field (`claims_physical_proximity`, `claims_knowledge_of_schedule`) and should escalate to human review automatically.
- Keep the raw comment, a screenshot or permalink, the timestamp, and the commenter handle and ID together with the classifier outputs, so the record is usable for legal review. Instagram API access and evidence-preservation rules are outside this note's scope.

### Gaps
- I found no published research quantifying how much commenter-history or location-reference features improve threat-detection accuracy.
- Professional threat-assessment frameworks for online threats (e.g., guidance from US Secret Service NTAC or law-enforcement escalation criteria) were not researched here and would strengthen the escalation logic.
