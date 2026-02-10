#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
expected_venv="${root_dir}/.venv"

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  echo "ERROR: No virtual environment is active."
  echo "Activate the project venv: source ${expected_venv}/bin/activate"
  exit 1
fi

active_venv="$(cd "${VIRTUAL_ENV}" && pwd)"
if [[ "${active_venv}" != "${expected_venv}" ]]; then
  echo "ERROR: Active venv is not the project root venv."
  echo "Active:  ${active_venv}"
  echo "Expected: ${expected_venv}"
  exit 1
fi

extra_venvs="$(
  find "${root_dir}" -maxdepth 4 -type d \
    \( -name ".venv" -o -name "venv" -o -name "env" -o -name "*.venv" -o -name "*venv*" \) \
    ! -path "${expected_venv}" \
    2>/dev/null
)"

if [[ -n "${extra_venvs}" ]]; then
  echo "WARNING: Additional venv-like directories detected:"
  echo "${extra_venvs}"
  echo "Consider removing them to avoid confusion."
fi

echo "OK: Using root venv at ${expected_venv}"
