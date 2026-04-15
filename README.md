# content_store

NCERT textbook ingestion pipeline: scrape PDFs → extract content via Gemini → persist to File Search stores.

## Structure

```
content_store/
├── inputs/          # Downloaded PDFs (grade/subject/book/chapter.pdf)
├── types.py         # Pydantic models: Book, Asset, PageMeta, ExtractedPage, Document
├── constants.py     # All constants: URLs, patterns, prompts, store kinds
├── scraper.py       # Scraper class: downloads NCERT PDFs
├── extractor.py     # Extractor class: splits PDFs, runs LLM extraction
├── persister.py     # Persister class: uploads to File Search stores
└── run.py           # Pipeline entrypoint
```

## Install

```bash
pip install -r content_store/requirements.txt
```

## Run

From the monorepo root (same pattern as other `run_*.py` scripts):

```bash
python run_content_store.py
```

Equivalent module entrypoint:

```bash
python -m content_store.run
```

## Environment

Required:

- `GOOGLE_CLOUD_PROJECT`
- `GOOGLE_CLOUD_LOCATION`
- `GEMINI_API_KEY` (via Secret Manager)
