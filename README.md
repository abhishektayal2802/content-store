# content_store

NCERT textbook ingestion pipeline: mirror raw PDFs to GCS, extract page content via OpenAI, and publish retrieval units to Vertex RAG corpora.

## Structure

```
content_store/
├── catalog.json         # Checked-in manifest of NCERT books we ingest
├── refresh_catalog.py   # Rebuilds catalog.json from ncert.nic.in
├── storage.py           # GCS object naming + content-store state IO
├── run_state.py         # GCS stage manifests + structured errors
├── types.py             # Pydantic models: Book, cached pages, run state, publish units
├── constants.py         # URLs, patterns, paths, concurrency limits
├── scraper.py           # Mirrors NCERT dd.zip chapter PDFs into raw/ GCS
├── extractor.py         # Splits raw GCS PDFs, runs OpenAI extraction per page
├── publisher.py         # Rebuilds Vertex RAG corpora from extracted/ GCS
└── run.py               # Pipeline entrypoint
```

## Install

```bash
pip install -e ./infra
pip install -r content_store/requirements.txt
```

## Refresh the NCERT catalog

The pipeline reads books from `catalog.json`. Regenerate it whenever NCERT
publishes new books (rare):

```bash
python -m content_store.refresh_catalog
```

This is the only code that talks to `ncert.nic.in/textbook.php`. Review the
resulting diff before committing.

### Catalog inclusion policy

`refresh_catalog.py` applies a generic curriculum-driven filter:

- **Content subjects** (math, science, etc.): only books in the chosen
  instruction language. Today that is English (`DEFAULT_INSTRUCTION_LANGUAGE` in
  `infra/curriculum/constants.py`, NCERT track code `e`).
- **Language subjects** (english, hindi, urdu, sanskrit): all NCERT textbooks
  for that subject, regardless of track letter.

To switch non-language content to another instruction medium (e.g. Hindi),
change `DEFAULT_INSTRUCTION_LANGUAGE` and re-run the refresh.

## Run the pipeline

From the monorepo root:

```bash
python run_content_store.py
```

Equivalent explicit stage entrypoints:

```bash
export CONTENT_STORE_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
python -m content_store.run scrape
python -m content_store.run extract
python -m content_store.run publish
```

The scraper downloads exactly one artefact per book: the `<code>dd.zip` bundle
served by NCERT. It keeps concurrency low, normalizes chapter names, and mirrors
chapter PDFs into the configured GCS bucket under `raw/`. Extraction writes page
JSON under `extracted/`. Publish stages run-scoped Vertex import files under
`runs/<run_id>/staging/`.

Cloud Run Jobs use the same stage entrypoint:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
gcloud run jobs execute content-store-scrape --region asia-south1 --project sujho-478914 --update-env-vars=CONTENT_STORE_RUN_ID="$RUN_ID" --wait
gcloud run jobs execute content-store-extract --region asia-south1 --project sujho-478914 --update-env-vars=CONTENT_STORE_RUN_ID="$RUN_ID" --wait
gcloud run jobs execute content-store-publish --region asia-south1 --project sujho-478914 --update-env-vars=CONTENT_STORE_RUN_ID="$RUN_ID" --wait
```

## Extraction model

Page extraction uses OpenAI structured output with:

- Model: `gpt-5.4-nano` (`Models.SMALL`)
- Reasoning: `medium`
- Verbosity: `low`

These defaults live in `constants.py` as `EXTRACTION_MODEL`, `EXTRACTION_REASONING_EFFORT`, and `EXTRACTION_VERBOSITY`.

## Environment

Required:

- `GOOGLE_CLOUD_PROJECT`
- `GOOGLE_CLOUD_LOCATION`
- `CONTENT_STORE_GCS_BUCKET`
- `OPENAI_API_KEY` in Secret Manager
