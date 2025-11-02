# Tests

Los tests están organizados siguiendo la estructura del proyecto, ubicados
junto a los módulos que testean.

## Ejecutar Tests

```bash
# Ejecutar todos los tests
make test

# Ejecutar con cobertura
make test-cov

# O directamente con pytest
pytest ingestor_scrapper/ -v
pytest ingestor_scrapper/ --cov --cov-report=html
```

## Estructura

```
ingestor_scrapper/
├── adapters/
│   ├── parsers/
│   │   └── tests/
│   │       └── test_bcra_excel_parser.py
│   └── normalizers/
│       └── tests/
│           └── test_bcra_monetario_normalizer.py
└── application/
    └── tests/
        └── test_bcra_monetario_use_case.py
```

## Cobertura Actual

**87%** de cobertura en los módulos principales.

