"""Local entry point: start the API on 127.0.0.1 with an ephemeral port."""

from __future__ import annotations

import uvicorn

from fleet.api import create_app
from fleet.service import RolloutService

app = create_app(RolloutService())


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
