#!/bin/bash
# Script para formatear y corregir automáticamente el código Python

source .venv/bin/activate

echo "🔍 Ejecutando ruff check --fix..."
ruff check --fix --unsafe-fixes ingestor_scrapper/

echo "✨ Formateando código con ruff format..."
ruff format ingestor_scrapper/

echo "✅ ¡Todo listo!"

