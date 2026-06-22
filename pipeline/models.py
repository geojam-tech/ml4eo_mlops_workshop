"""CFAR detectors as MLflow models.

A CFAR "model" is a parameter set, not learned weights. This is an unsupervised
detector, so a model is fully defined by its config. We content-hash that config
into a short `version` sha, register it as an MLflow pyfunc model, and load it
back by sha. Identical params always produce the same sha, which is what keeps
skip-if-present correct: re-promoting the same config is a no-op, and the
pipeline keys its results on the sha.
"""

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, asdict
from pathlib import Path

import mlflow
import mlflow.pyfunc
from mlflow.tracking import MlflowClient

REGISTERED_NAME = "cfar"
PIPELINE_DIR = str(Path(__file__).resolve().parent)


@dataclass(frozen=True)
class CFARConfig:
    pfa: float = 1e-4
    guard: int = 5
    background: int = 15
    min_pixels: int = 3
    max_pixels: int = 500


# The detectors the workshop compares: a strict one (low false-alarm rate, the
# default) and a loose one (higher rate). Setup registers only the first; the
# loose one is promoted by hand in the notebook (the interactive step).
DEMO_CONFIGS = [
    CFARConfig(pfa=1e-4),
    CFARConfig(pfa=1e-2),
]


def config_sha(cfg: CFARConfig) -> str:
    payload = json.dumps(asdict(cfg), sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:8]


class CFARModel(mlflow.pyfunc.PythonModel):
    """Wraps pipeline.detector.detect with its config baked in.

    predict input: {"image_db": ndarray, "water_mask": ndarray | None}
    predict output: {"count": int, "detections": [(row, col, size_pixels), ...]}
    """

    def load_context(self, context):
        with open(context.artifacts["config"]) as f:
            self.cfg = json.load(f)

    def predict(self, context, model_input, params=None):
        from pipeline.detector import detect
        image_db = model_input["image_db"]
        water_mask = model_input.get("water_mask")
        dets = detect(image_db, water_mask, **self.cfg)
        return {"count": len(dets), "detections": dets}


def _find_version(client: MlflowClient, sha: str):
    """The registry version number whose config_sha tag matches, or None."""
    try:
        for mv in client.search_model_versions(f"name='{REGISTERED_NAME}'"):
            if mv.tags.get("config_sha") == sha:
                return mv.version
    except Exception:
        return None
    return None


def register(cfg: CFARConfig, alias: str | None = None) -> str:
    """Register a CFAR config as an MLflow model version. Idempotent by sha.

    If `alias` is given, point that registry alias at this version (e.g.
    'default'). The alias is a convenience pointer in MLflow; the pipeline still
    keys everything on the sha.
    """
    sha = config_sha(cfg)
    client = MlflowClient()
    version = _find_version(client, sha)

    if version is None:
        with tempfile.TemporaryDirectory() as d:
            cfg_path = os.path.join(d, "config.json")
            with open(cfg_path, "w") as f:
                json.dump(asdict(cfg), f)
            mlflow.set_experiment("cfar")
            with mlflow.start_run(run_name=f"cfar-{sha}"):
                mlflow.log_params(asdict(cfg))
                mlflow.set_tag("config_sha", sha)
                mlflow.pyfunc.log_model(
                    artifact_path="cfar",
                    python_model=CFARModel(),
                    artifacts={"config": cfg_path},
                    code_paths=[PIPELINE_DIR],
                    registered_model_name=REGISTERED_NAME,
                )
        version = _find_version(client, sha)
        if version is None:  # the just-registered version needs the tag
            version = max(int(mv.version) for mv in client.search_model_versions(
                f"name='{REGISTERED_NAME}'"))
            client.set_model_version_tag(REGISTERED_NAME, str(version), "config_sha", sha)

    if alias:
        client.set_registered_model_alias(REGISTERED_NAME, alias, str(version))
    return sha


def load(sha: str):
    """Load a registered CFAR model by its config sha."""
    version = _find_version(MlflowClient(), sha)
    if version is None:
        raise ValueError(f"no '{REGISTERED_NAME}' model with config_sha={sha}; register it first")
    return mlflow.pyfunc.load_model(f"models:/{REGISTERED_NAME}/{version}")


def resolve_version(name: str) -> str:
    """Resolve an alias (e.g. 'default') or a sha to the concrete config sha.

    Results are always keyed on the sha, so an alias is just convenient input.
    """
    try:
        mv = MlflowClient().get_model_version_by_alias(REGISTERED_NAME, name)
        return mv.tags.get("config_sha") or name
    except Exception:
        return name  # not an alias; assume it is already a sha (load validates)


def list_aliases() -> list[str]:
    """Alias names registered on the cfar model (e.g. ['default'])."""
    try:
        return list(MlflowClient().get_registered_model(REGISTERED_NAME).aliases.keys())
    except Exception:
        return []


def list_versions() -> list[dict]:
    """All registered CFAR versions with their config sha and run id."""
    client = MlflowClient()
    out = []
    for mv in client.search_model_versions(f"name='{REGISTERED_NAME}'"):
        out.append({"version": mv.version, "config_sha": mv.tags.get("config_sha"),
                    "run_id": mv.run_id})
    return out
