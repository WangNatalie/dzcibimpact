"""litreview — ecosystem-services valuation literature review pipeline.

Three-tier funnel:
  tier 1  global         abstract-level counts + ESVD/TEEB value averages
  tier 2  north america  finer method/ecosystem cross-tabs, abstract extraction
  tier 3  carolinian     full-text extraction into the target output table

OpenAlex is the counting backbone; Semantic Scholar enriches abstracts/PDFs.
"""

__version__ = "0.1.0"
