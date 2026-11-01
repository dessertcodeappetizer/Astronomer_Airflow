"""Register YAML-defined Dags with Airflow (see *.dag.yaml next to this file)."""

from blueprint import build_all

build_all()