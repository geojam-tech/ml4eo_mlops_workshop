"""Register flows with the Prefect server so they can be triggered from the UI.

Three deployments:
    setup:        rebuild the foundations (schema, site, water mask, AIS, models)
    monitoring:   run a CFAR model over a date window, day by day
    process-day:  run a CFAR model over a single day

Parameters use the flows' own defaults: the 'default' model alias (resolved to a
sha at run time) and the full date window.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from prefect import serve

from pipeline.setup import setup_pipeline
from pipeline.flows import monitoring, process_day

if __name__ == "__main__":
    serve(
        setup_pipeline.to_deployment(name="setup"),
        monitoring.to_deployment(name="monitoring"),
        process_day.to_deployment(name="process-day"),
    )
