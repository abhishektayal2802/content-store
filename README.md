# content_store

NCERT textbook ingestion pipeline: download per-book zip bundles → extract page content via Gemini → persist to File Search stores.

## Structure

```
content_store/
├── inputs/              # Unpacked PDFs (grade/subject/book/chapter.pdf)
│   └── _zips/           # Cached per-book NCERT zip bundles (resumable)
├── catalog.json         # Checked-in manifest of NCERT books we ingest
├── refresh_catalog.py   # Rebuilds catalog.json from ncert.nic.in
├── types.py             # Pydantic models: Book, ExtractionSlice, Stage
├── constants.py         # URLs, patterns, paths, concurrency limits
├── scraper.py           # Downloads dd.zip per book via pypdl, unpacks PDFs
├── extractor.py         # Splits PDFs, runs Gemini extraction per page
├── persister.py         # Uploads extracted docs to File Search stores
├── indexer.py           # Polls File Search upload ops until indexed
└── run.py               # Pipeline entrypoint
```

## Install

```bash
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

## Run the pipeline

From the monorepo root:

```bash
python run_content_store.py
```

Equivalent module entrypoint:

```bash
python -m content_store.run
```

The scraper downloads exactly one artefact per book: the `<code>dd.zip` bundle
served by NCERT. Downloads are resumable (pypdl handles HTTP Range + ETag),
retry automatically on the flaky NCERT host, and cache in `inputs/_zips/`.
Re-running the pipeline with an already-populated `inputs/<grade>/.../<book>/`
directory skips the download entirely.

## Environment

Required:

- `GOOGLE_CLOUD_PROJECT`
- `GOOGLE_CLOUD_LOCATION`
- `GEMINI_API_KEY` (via Secret Manager)
