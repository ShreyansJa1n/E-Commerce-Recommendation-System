# Experiment: LambdaRank re-ranker vs. the priority blend

Offline experiment on the held-out **test** period (cutoff 2015-09-04, labels 2015-09-04 to 2015-09-17). All numbers come from `make eval ENV=base` (`data/gold/_reports/evaluation_test.json`). The generated tables are in [EVAL_REPORT.md](EVAL_REPORT.md).

## Hypothesis
Re-ranking the merged candidates with a LambdaRank model (ADR-010) increases NDCG@10 over the best non-learned policy: a priority blend of the visitor's recent items, then category popularity, then global popularity.

- **Primary metric:** per-visitor NDCG@10, graded relevance (view = 1, cart = 2, purchase = 3), averaged over the 142,838 visitors with ≥ 1 interaction in the test window. 14,839 of them have history before the cutoff.
- **Secondary metrics:** Recall@20, HR@10.
- **Guardrails:** catalog coverage, intra-list diversity, novelty.
- **Decision rule, set before looking:** ship to a segment only if the paired 95% CI of the NDCG@10 difference excludes 0 in the ranker's favor there.

## Design
- **Paired (offline counterfactual):** both policies scored on every visitor, with visitors resampled 2,000 times. This has maximum power and no assignment noise.
- **Simulated A/B:** visitors hashed 50/50 (salt `ab-sim-2026-10`, treatment share 0.502). Each arm is scored only with its own policy, and the arms are resampled independently. This is what an online test of the same traffic would observe.
- **Not a true replay or counterfactual estimate.** Retailrocket logged no recommendations or propensities, so "treatment" means "how well would this list have matched what the visitor later did". It ignores the effect a list has on behavior (ADR-011).

## Results: NDCG@10
| comparison | segment | control | treatment | paired lift (95% CI) | sig. | simulated A/B lift (95% CI) | sig. |
|---|---|---:|---:|---|---|---|---|
| ranker vs blend | all | 0.0229 | 0.0229 | -0.0% [-0.6%, +0.7%] | no | +2.4% [-3.5%, +8.4%] | no |
| ranker vs blend | warm | 0.1765 | 0.1805 | +2.3% [+1.8%, +2.7%] | yes | +5.6% [-1.0%, +12.7%] | no |
| ranker vs blend | cold | 0.0051 | 0.0046 | -9.1% [-11.5%, -6.6%] | yes | -6.9% [-17.8%, +4.4%] | no |
| ranker vs popular_global | all | 0.0051 | 0.0229 | +353.1% [+329.7%, +377.6%] | yes | +356.7% [+319.3%, +400.1%] | yes |
| ranker vs popular_global | warm | 0.0046 | 0.1805 | +3820.7% [+3189.0%, +4609.3%] | yes | +3157.5% [+2512.9%, +4068.6%] | yes |
| ranker vs popular_global | cold | 0.0051 | 0.0046 | -9.1% [-11.5%, -6.6%] | yes | -6.9% [-17.8%, +4.4%] | no |
| ranker vs als | all | 0.0036 | 0.0229 | +540.8% [+497.8%, +587.3%] | yes | +590.2% [+517.2%, +677.1%] | yes |
| ranker vs als | warm | 0.0344 | 0.1805 | +424.5% [+389.5%, +461.2%] | yes | +470.6% [+411.9%, +541.5%] | yes |
| ranker vs als | cold | 0.0000 | 0.0046 | n/a [n/a, n/a] | yes | n/a [n/a, n/a] | yes |
| ranker vs recent_items | all | 0.0175 | 0.0229 | +30.6% [+28.9%, +32.5%] | yes | +33.4% [+24.7%, +42.3%] | yes |
| ranker vs recent_items | warm | 0.1688 | 0.1805 | +6.9% [+6.3%, +7.6%] | yes | +10.3% [+3.2%, +17.8%] | yes |
| ranker vs recent_items | cold | 0.0000 | 0.0046 | n/a [n/a, n/a] | yes | n/a [n/a, n/a] | yes |

Secondary metrics, ranker vs blend (paired):

| metric | segment | blend | ranker | lift (95% CI) | sig. |
|---|---|---:|---:|---|---|
| Recall@20 | all | 0.0316 | 0.0360 | +14.2% [+12.6%, +15.8%] | yes |
| Recall@20 | warm | 0.2008 | 0.2110 | +5.1% [+4.1%, +6.0%] | yes |
| Recall@20 | cold | 0.0119 | 0.0158 | +32.0% [+27.8%, +36.8%] | yes |
| HR@10 | all | 0.0329 | 0.0346 | +5.2% [+4.0%, +6.5%] | yes |
| HR@10 | warm | 0.2245 | 0.2321 | +3.4% [+2.5%, +4.4%] | yes |
| HR@10 | cold | 0.0107 | 0.0117 | +9.6% [+6.2%, +13.3%] | yes |

Beyond accuracy (top-10):

| policy | catalog coverage | distinct items | diversity | novelty (bits) | median popularity rank |
|---|---:|---:|---:|---:|---:|
| `ranker` | 6.72% | 31,179 | 0.919 | 10.45 | 7 |
| `blend` | 6.08% | 28,203 | 0.862 | 10.73 | 6 |
| `recent_items` | 5.38% | 24,950 | 0.563 | 15.80 | 230 |
| `popular_category` | 1.42% | 6,596 | 0.092 | 14.38 | 209 |
| `als` | 1.72% | 7,985 | 0.366 | 14.13 | 204 |
| `item2vec` | 7.29% | 33,821 | 0.399 | 16.56 | 234 |
| `cooccurrence` | 3.50% | 16,223 | 0.349 | 15.07 | 222 |
| `popular_global` | 0.00% | 10 | 0.933 | 10.31 | 5 |

![lift](figures/lift_forest.png)

## Findings
1. **The ranker beats popularity, ALS and recent items with intervals far from zero:** +353%, +541% and +31% NDCG@10 (all visitors, paired).
2. **Against the blend it is a tie overall** (-0.0% [-0.6%, +0.7%]). That tie hides two opposite, significant effects:
   - **Returning visitors:** +2.3% [+1.8%, +2.7%]
   - **New visitors:** -9.1% [-11.5%, -6.6%]. For cold visitors, the ranker's reordering of the popularity list by item features generalizes worse than plain popularity order.
3. **A 50/50 online A/B test at this traffic would not detect the returning-visitor gain.** The simulated A/B interval for returning visitors is +5.6% [-1.0%, +12.7%] and includes 0. With ~7,429 returning visitors per arm per two-week window, the minimum detectable effect at 80% power is about 9% relative. Detecting the measured +2.3% would need about 17× the sample (~127,956 returning visitors per arm), or a variance-reduction method such as CUPED or interleaving.
4. **Guardrails:** the ranker's top-10 cover 6.72% of the catalog vs 6.08% for the blend, with similar diversity (0.919 vs 0.862). Both lean on popular items (median popularity rank 7 vs 6), because 90% of lists go to cold visitors.

## Decision
- **Returning visitors: ship the ranker.** The paired CI excludes 0 (+1.8% to +2.7%) and the guardrails hold.
- **New visitors: keep the blend's popularity order.** The ranker is significantly worse (−6.6% to −11.5%).
- **Serving policy (Phase 7):** ranker lists for visitors with history, global popularity for everyone else. This hybrid comes from the experiment's result, so it has not itself been evaluated on unseen data. The next experiment period should confirm it.
- **Online validation:** this lift is too small for a 50/50 A/B at this traffic. Use interleaving or a longer, CUPED-adjusted test.
