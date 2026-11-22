# Data access routes for collecting comments on a public Instagram creator's posts (US, as of Nov 2026)

Starting case: @philgaimon (verified, Creator/Athlete professional account, ~8,100 posts, ~120K followers). Purpose: detect and preserve violent threats against the creator, not dox commenters.

## Q1. Official Instagram API: endpoints, permissions, fields, limits, moderation

### Takeaway
The official Instagram API, used with the creator's own login, is the best lawful route. Webhooks plus a one-time backfill cover new and historical comments. Endpoints return comment ID, text, timestamp, commenter username and IG-scoped ID, `parent_id` and replies, and the hidden flag. The same token can hide or delete comments. Main blind spots: comments from Restricted users, comments on age-gated media, live-video comments after the broadcast ends, and comments that are already deleted.

### Cited Findings
**Endpoints**
- `GET /{ig-media-id}/comments` returns top-level comments in reverse chronological order. Replies come back only through field expansion (`replies`). Max 50 comments per query. Results can't be filtered by timestamp. Live video isn't supported. On boosted posts, comments on both the organic post and the ad come back. — [Meta: IG Media Comments reference](https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-media/comments)
- `GET /{ig-media-id}/ordered_comments` returns comments in Instagram's display order and allows `replies` and `from` expansion. Facebook Login only. Not for stories or carousel children. — [same](https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-media/comments)
- The moderation endpoint set:
  - `GET /{ig-comment-id}/replies` reads replies.
  - `POST /{ig-comment-id}/replies` posts a reply.
  - `POST /{ig-comment-id}?hide=true|false` hides or unhides a comment.
  - `DELETE /{ig-comment-id}` deletes a comment.
  - `POST /{ig-media-id}` turns comments on or off for a post.
  - Source: [Meta: Comment Moderation guide](https://developers.facebook.com/docs/instagram-platform/comment-moderation)
- IG Comment fields:
  - `from` (commenter `id` = IGSID, plus `username`)
  - `username`, `hidden`, `like_count`, `parent_id`, `replies`
  - `timestamp` (ISO 8601), `legacy_instagram_comment_id`
  - `user` comes back only when the app user wrote the comment.
  - Since Aug 27, 2024, reading `username` for other commenters needs `instagram_manage_comments` or `instagram_business_manage_comments`.
  - Source: [Meta: IG Comment reference](https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-comment)
- Documented read limitations (same source, [IG Comment reference](https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-comment)):
  - Comments on age-gated media are not returned.
  - Comments from users the app user has Restricted are not returned unless the user is unrestricted and the comment approved.
  - Live-video comments can be read only during the broadcast.
  - The docs say nothing about deleted comments.
- Hiding needs the media owner's token. The owner's own comments always stay visible. Docs conflict on who can delete: one statement says the commenter's token, another says only the media owner can delete. — [IG Comment reference](https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-comment)
- Webhooks: subscribe to the `comments` and `live_comments` fields.
  - Payloads carry `from.id`, `from.username`, comment ID, media ID and text.
  - With Facebook Login, payloads also carry `parent_id` for replies and ad fields.
  - Meta recommends webhooks over polling to avoid rate limits.
  - Source: [Comment Moderation guide](https://developers.facebook.com/docs/instagram-platform/comment-moderation)
- Media enumeration: `GET /{ig-user-id}/media` returns at most the 10K most recently created media. It supports `since`/`until` time pagination and excludes stories. — [Meta: IG User Media reference](https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-user/media)

**Permissions and access**
- Instagram API with Instagram Login (`graph.instagram.com`, IG user token) needs `instagram_business_basic` + `instagram_business_manage_comments`.
- Facebook Login (`graph.facebook.com`, Page token) needs `instagram_basic` + `instagram_manage_comments` + `pages_read_engagement`, plus `ads_management`/`ads_read` when the user's Page role comes through Business Manager.
- Standard Access covers accounts you own or manage that are added in the App Dashboard. Advanced Access (via App Review) is needed for accounts you don't own or manage.
- Source: [Comment Moderation guide](https://developers.facebook.com/docs/instagram-platform/comment-moderation)

**Rate limits**
- Instagram Platform (non-messaging): "Calls within 24 hours = 4800 * Number of Impressions". The count is per app–user pair over a rolling 24 hours. Impressions are the times the account's content entered someone's screen in the last 24 hours. — [Meta: Rate limiting](https://developers.facebook.com/docs/graph-api/overview/rate-limiting)

### Inferences
- Backfill scale: ~8,100 posts is under the 10K media cap, so the whole post history should be listable. One account-level pull is one `/media` page walk plus at least one `/comments` call per post (more per 50 comments, plus reply expansion). That totals roughly 10^4 to 10^5 calls. For an account with ~120K followers, daily impressions are likely in the tens or hundreds of thousands, so 4800 × impressions should be far above what's needed. This is an estimate; Gaimon's impression count wasn't checked.
- The docs I fetched don't say how many years back comments stay retrievable. Nothing suggests an age cutoff on old posts. Comments a user or the creator deleted are almost certainly gone from the API.
- Since `/comments` returns no permalink field, build the permalink from the media `permalink` plus the comment ID, or store the media permalink alongside it. I haven't confirmed that a comment-level permalink field exists.
- Restricted-user comments are excluded. These may be exactly the abusive accounts that matter. The creator may need to unrestrict before a backfill, or capture those comments manually in the app.
- Hidden Words: the `hidden` boolean exists, so comments hidden by the owner or app are probably returned with `hidden=true`. Whether comments auto-filtered by Hidden Words or Limits are returned is not documented (see Gaps).

### Gaps
- No Meta doc found on whether Hidden Words or Limits-filtered comments appear in `/comments` or in webhooks.
- No documented page-size or pagination detail for `/comments` beyond the 50-per-query maximum.
- No documented history depth for comments, and no confirmation of a comment-level permalink field.

## Q2. Basic Display API status in 2026

### Takeaway
Gone. Meta shut down the Instagram Basic Display API on Dec 4, 2024, after announcing it Sept 4, 2024. The replacement is the Instagram API with Instagram Login, which serves only Business/Creator (professional) accounts. Gaimon's Creator account qualifies.

### Cited Findings
- Meta announced the deprecation on Sept 4, 2024. All Basic Display requests fail from Dec 4, 2024, and Meta recommends migrating to the Instagram API. — [Meta developer blog](https://developers.facebook.com/blog/post/2024/09/04/update-on-instagram-basic-display-api); [TechCrunch, Dec 6, 2024](https://techcrunch.com/2024/12/06/instagram-locks-out-developers-of-third-party-consumer-apps)
- Third parties report that the replacement doesn't support personal accounts. — [Zapier help](https://help.zapier.com/hc/en-us/articles/32429170578317-Instagram-app-deprecation-on-Dec-4-2024). One vendor ([Smash Balloon](https://smashballoon.com/instagram-is-shutting-down-basic-display-api-continue-displaying-instagram-feeds-on-your-site/)) claims personal accounts still work, which conflicts with Zapier.

### Inferences
- Basic Display never exposed other users' comments anyway, so its loss doesn't affect this use case.

### Gaps
- I didn't fetch a 2026 Meta changelog. Instagram API endpoints or permission names may have changed since the pages fetched (Nov 2026 versions of the docs).

## Q3. Can a third party (nonprofit or app) run a multi-creator service? Platform Terms on retention, use and law-enforcement sharing

### Takeaway
Yes, technically. A nonprofit app can onboard many creators through Instagram Login once App Review grants Advanced Access, acting as a Tech Provider processing data "on behalf of and at the direction of" each creator. The catch is that Meta Platform Terms §3.a.iii prohibits processing Platform Data "for law enforcement or national security purposes" as "surveillance", with no stated safety exception. The service must be framed as the creator's own moderation and safety tool. Disclosures to police should go through the creator's express direction, or happen only when legally required, and leave a paper trail.

### Cited Findings (Meta Platform Terms, last updated Feb 3, 2026 — [developers.facebook.com/terms](https://developers.facebook.com/terms/))
- **§3.a.iii** prohibits "Processing Platform Data to perform, facilitate, or provide tools for surveillance". Surveillance "includes the Processing of Platform Data about people, places, groups, or events ... for law enforcement or national security purposes." No safety exception is stated.
- **§3.a.iv** prohibits selling, licensing or purchasing Platform Data.
- **§3.c** allows sharing only:
  1. when required by law (keep proof);
  2. with your Service Provider;
  3. when a User "expressly directs you to share or expressly consents" (keep proof of the consent and its purpose);
  4. with other third parties, for non-Restricted Platform Data only, under contractual restrictions.
  - Data collected as a Tech Provider follows §5.b.
- **§5.b.ii.1** says Tech Providers "will only use Platform and Process Platform Data on behalf of and at the direction of your Client" and not for their own purposes or another client's.
- **§3.d** requires deleting data when no longer needed for a legitimate business purpose, when the service stops, or on request by Meta or the User. Data that must be kept by law can be kept, with proof.
- Advanced Access is needed to manage comments for accounts the developer doesn't own or manage. — [Comment Moderation guide](https://developers.facebook.com/docs/instagram-platform/comment-moderation)

### Inferences
- The framing below is my reading of the Terms, not legal advice.
  - Permitted framing: a moderation and safety-triage tool for the creator, who decides what to export and send to police (§3.c.ii.3, express direction).
  - Risky framing: a service whose stated purpose is feeding law enforcement, or one that pools commenter data across creators to profile individuals (§3.a.iii, §5.b.ii.1).
- Cross-creator profiling (e.g., "this username threatened five athletes") is likely barred for a Tech Provider, because data from Client A can't be used for Client B.
- The retention policy should define an evidence-preservation period tied to the creator's direction and any legal hold, and delete non-flagged comments promptly.
- App Review has to approve the use-case description. Expect scrutiny of "threat detection" wording.

### Gaps
- No Meta guidance found specifically on safety or harassment-monitoring apps under §3.a.iii. The §3.e exemption list (an external page) wasn't reviewed.

## Q4. Scraping law (US): hiQ, Van Buren, Meta v. Bright Data, X Corp v. Bright Data; logged-in risk

### Takeaway
Under current Ninth Circuit law, scraping publicly visible, logged-out pages is unlikely to violate the CFAA (hiQ, Van Buren). After Meta v. Bright Data (N.D. Cal. Jan 2024), Meta's terms probably don't bind logged-out non-users. Logged-in scraping is a different matter: hiQ ultimately lost on breach of contract, with fake accounts central, and paid a $500K consent judgment. Meta's Terms expressly bar automated collection without permission. Account bans, contract claims and possibly trespass-to-chattels claims (revived in X v. Bright Data) remain real risks. None of this is needed when the creator authorizes API access.

### Cited Findings
- **Van Buren v. United States** (U.S. June 3, 2021): "exceeds authorized access" in the CFAA is a gates-up-or-down test of whether you can access parts of a system. It doesn't cover misuse of information you're entitled to access. — [Supreme Court opinion](https://www.supremecourt.gov/opinions/20pdf/19-783_k53l.pdf)
- **hiQ v. LinkedIn**: the Ninth Circuit (2022, after remand in light of Van Buren) held that violating a public website's user agreement alone isn't access "without authorization" under the CFAA. — [Morgan Lewis](https://www.morganlewis.com/blogs/sourcingatmorganlewis/2022/12/linkedin-v-hiq-landmark-data-scraping-suit-provides-guidance-to-data-scrapers-and-web-operators); [Wikipedia summary](https://en.wikipedia.com/wiki/HiQ_Labs_v._LinkedIn)
- **hiQ, later stages**:
  - Nov 2022: the district court found hiQ breached LinkedIn's User Agreement through automated scraping and by using crowdworkers to create fake profiles, and declined to find the agreement unconscionable.
  - Dec 2022: stipulated consent judgment of $500K covering breach of contract, CFAA and California unauthorized-access claims, plus a permanent injunction requiring hiQ to stop scraping and delete its data and code.
  - Sources: [Proskauer](https://newmedialaw.proskauer.com/2022/12/08/hiq-and-linkedin-reach-proposed-settlement-in-landmark-scraping-case/); [LinkedIn newsroom](https://news.linkedin.com/2022/november/court-order-affirms-linkedin-s-legal-positions-against-hiq-in-da); [ZwillGen](https://www.zwillgen.com/alternative-data/hiq-v-linkedin-wrapped-up-web-scraping-lessons-lea)
- **Meta v. Bright Data** (N.D. Cal., Judge Chen, Jan 23, 2024):
  - Summary judgment for Bright Data on breach of contract.
  - Meta's Terms bind "users" (account holders), not logged-out visitors.
  - Bright Data's logged-out scraping was unrelated to the purpose of its accounts.
  - The survival clause couldn't reach post-termination scraping because it had no time or scope limit.
  - The record showed no logged-in scraping.
  - Sources: [Goldman blog](https://blog.ericgoldman.org/archives/2024/01/game-on-bright-data-scores-major-victory-in-web-scraping-dispute-with-meta-guest-blog-post.htm); [TechCrunch](https://techcrunch.com/2024/01/24/court-rules-in-favor-of-a-web-scraper-bright-data-which-meta-had-used-and-then-sued)
- In Feb 2024 Meta dismissed its remaining tortious-interference claim without prejudice and waived appeal. Bright Data said there was no settlement and it wouldn't change its conduct. — [TechCrunch, Feb 26, 2024](https://techcrunch.com/2024/02/26/meta-drops-lawsuit-against-web-scraping-firm-bright-data-that-sold-millions-of-instagram-records/)
- Commentators note the ruling turns on Meta's specific wording. Meta could revise its terms. CFAA and trespass-to-chattels theories weren't tested. — [Goldman blog](https://blog.ericgoldman.org/archives/2024/01/game-on-bright-data-scores-major-victory-in-web-scraping-dispute-with-meta-guest-blog-post.htm)
- **X Corp v. Bright Data** (N.D. Cal., Judge Alsup, No. 3:23-cv-03698):
  - May 2024: dismissed. Contract claims over scraping public data were held preempted by the Copyright Act, and no server harm was adequately alleged. — [CNBC](https://www.cnbc.com/2024/05/10/elon-musks-x-loses-lawsuit-against-bright-data-over-data-scraping.html); [Skadden](https://skadden.com/insights/publications/2024/05/district-court-adopts-broad-view)
  - Nov 26, 2024 (Dkt. 156): X was allowed to proceed on server-impairment theories (trespass to chattels, related contract theory) and some §17200 and tortious-interference theories. Data-scraping contract theories stayed preempted. DMCA, CFAA and CDAFA claims were allowed in, unopposed. Bright Data counterclaimed, including antitrust claims. — [Goldman blog, Jan 2025](https://blog.ericgoldman.org/archives/2025/01/catching-up-on-the-heavyweight-scraping-battle-between-x-and-bright-data-guest-blog-post.htm); [Bloomberg Law](https://news.bloomberglaw.com/litigation/x-partially-revives-lawsuit-against-israeli-data-scraping-firm); [order](https://storage.courtlistener.com/recap/gov.uscourts.cand.415869/gov.uscourts.cand.415869.156.0.pdf)
- **Instagram Terms of Use** bar users from "creating accounts or collecting information in an automated way without our express permission". Meta's Automated Data Collection Terms likewise require express written permission. — [Instagram Terms (Help Center)](https://help.instagram.com/581066165581870); [Thunderbit commentary](https://thunderbit.com/blog/is-scraping-instagram-legal)

### Inferences
- **Logged-out scraping** of a public creator's comments carries low CFAA risk in the Ninth Circuit, and contract risk is weakened after Meta v. Bright Data. It's still technically fragile, and Meta can block IPs or send cease-and-desist letters. Trespass to chattels survives as a theory where server load is shown. A small, low-rate collection on one account is unlikely to meet that bar.
- **Logged-in scraping**, whether with the creator's own account or a burner, breaches the Instagram Terms the account holder agreed to. It risks bans on the creator's account itself, which is the asset you're trying to protect, and it's exactly the conduct hiQ lost on. Avoid it.
- **Evidence:** API-sourced records with Meta IDs, timestamps and a chain of custody are likely more defensible than scraped HTML. This is an inference, not verified against evidence law sources.

### Gaps
- I couldn't confirm the X Corp v. Bright Data status in 2025–2026 (trial, settlement or appeal).
- No 2025–2026 appellate ruling on Meta's terms and logged-out scraping was found.
- I couldn't retrieve the current Instagram Terms' effective date (the help page didn't render).

## Q5. Commercial providers: cost per 1,000 comments (2026)

### Takeaway
Roughly $0.75–$2.60 per 1,000 comments. Apify's official Instagram Comments Scraper costs $1.90–$2.60/1K depending on plan. Bright Data advertises its Scraper API from $0.75/1K records, and pre-collected datasets at about $2.50/1K with a $250 minimum. These tools scrape only the logged-out view, so coverage is incomplete.

### Cited Findings
- **Apify `apify/instagram-comment-scraper`**:
  - Pricing: "from $1.90 / 1,000 comments"; $2.60/1K on the Free plan; $2.30/1K on Starter ($19/mo).
  - Outputs: comment ID, post ID, text, timestamp, owner ID and username, likes, reply count, nested replies, comment URL.
  - Nested replies are a paid-only option, and each reply bills as a separate result.
  - It collects only what logged-out users see.
  - Source: [Apify actor page](https://apify.com/apify/instagram-comment-scraper)
- An Apify issue thread reports getting only 15 of 640 comments. Logged-out coverage can be very partial. — [Apify issue](https://apify.com/apify/instagram-comment-scraper/issues/only-15-of-the-640-c-msFFNfC154z2j70si) (title only; not fetched)
- **Bright Data**:
  - Instagram Comments dataset: up to $0.0025 per record (~$2.50/1K), $250 minimum. — [Bright Data dataset page](https://brightdata.com/products/datasets/instagram/comments)
  - Comments extractor: Scraper API "from $0.75 per 1K records". — [Bright Data extractor](https://brightdata.com/solutions/data-extraction/instagram/comments)
  - Generic social-media scraper: $1.5/1K pay-as-you-go. — [Bright Data social scraper](https://brightdata.com/products/datasets/social-media-scrape)
  - Bright Data's own pages conflict on figures. Prices weren't verified via the pricing page or sales.

### Inferences
- Cost isn't the deciding factor. If Gaimon's 8,100 posts average ~50–150 comments each (unverified), that's ~400K–1.2M comments, or ~$300–$3,000 one time.
- The binding constraints for vendor scraping are completeness (logged-out view, missing replies, posts that won't load) and Meta Platform Terms issues: buying data "purchased" from scrapers isn't Platform Data, but it does undercut the creator's own API relationship.
- Vendor data also carries no `hidden` flag and no moderation ability.

### Gaps
- No independent 2026 benchmark of vendor completeness for Instagram comments was found.

## Q6. Manual and export routes; practical caveats (deleted, hidden, restricted)

### Takeaway
Instagram's "Download your information" export is documented as including comments the account holder made. I found no source saying it includes comments others left on the creator's posts. It's slow (up to 30 days) and isn't a monitoring tool. In-app moderation (Restrict, Hidden Words, Limits, delete) hides content from the public and from the API. A collection pipeline must run before or alongside those filters, and the creator should screenshot any threats before deleting them.

### Cited Findings
- The data download includes comments you've made. Instagram may take up to 30 days to email the link, and one guide says the link lasts 4 days. No source says it includes others' comments on your posts. — [MakeTechEasier](https://maketecheasier.com/see-all-comments-instagram/); [PIRG guide](https://pirg.org/resources/how-to-request-and-download-instagram-data/); [Instagram Help](https://help.instagram.com/181231772500920)
- After you Restrict someone, their comments on your posts are visible only to them. Hidden Words offers a "Hide comments" toggle and "Advanced comment filtering". — [SWGfL Instagram checklist 2024](https://www.swgfl.org.uk/assets/documents/instagram-checklist-2024.pdf)
- The API doesn't return Restricted users' comments, and returns live comments only during the broadcast. — [IG Comment reference](https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-comment)

### Inferences
- **Deleted comments:** gone from the API and from scrapers once deleted. Preservation means capturing at ingestion through webhooks before anyone moderates. If police are involved, Meta can be served legal process or a preservation request. That route wasn't researched here.
- **Hidden Words:** automatically filtered comments may be among the most threatening. Whether the API exposes them is undocumented, so test on a live account by posting a filtered keyword from a test account and checking `/comments` and the webhook.
- **Order of operations:** connect the API and webhooks first, then backfill, and only then hide or delete through the API (`hide=true` keeps the record). Hiding beats deleting for evidence purposes.

### Gaps
- No authoritative Meta source on whether DYI includes others' comments on your posts, or Hidden Words-filtered comments. The best way to settle it is to request a download from a test account.
- Meta's law-enforcement preservation and records process wasn't researched.
