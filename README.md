# pipeweave

Lightweight Python library for composing async data transformation pipelines with built-in retry and error handling.

---

## Installation

```bash
pip install pipeweave
```

---

## Usage

```python
import asyncio
from pipeweave import Pipeline, step

@step(retries=3)
async def fetch_data(url: str) -> dict:
    # simulate fetching data
    return {"url": url, "data": "..."}

@step()
async def transform(payload: dict) -> dict:
    return {**payload, "processed": True}

@step()
async def save(payload: dict) -> None:
    print(f"Saving: {payload}")

async def main():
    pipeline = Pipeline([fetch_data, transform, save])
    await pipeline.run("https://example.com/api")

asyncio.run(main())
```

Pipelines short-circuit on unrecoverable errors and surface structured exceptions, so you always know exactly where and why a stage failed.

---

## Features

- **Composable** — chain async steps with a clean, declarative API
- **Retry logic** — per-step configurable retries with exponential backoff
- **Error handling** — structured exceptions with full stage context
- **Lightweight** — zero required dependencies beyond the Python standard library

---

## License

Released under the [MIT License](LICENSE).