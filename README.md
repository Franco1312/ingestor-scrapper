# ingestor-scrapper

Un proyecto de Scrapy con Clean Architecture (Ports & Adapters) para aprender web scraping desde cero, pero con una estructura escalable y ordenada desde el inicio.

## 📋 Descripción

Este proyecto implementa un scaffold mínimo pero funcional de Scrapy siguiendo los principios de Clean Architecture (Hexagonal Architecture). La estructura está diseñada para crecer sin necesidad de reestructurar todo el proyecto cuando se agreguen nuevas funcionalidades.

## 🏗️ Arquitectura

El proyecto está organizado en capas siguiendo Clean Architecture:

```
ingestor_scrapper/
├─ core/                    # Dominio (framework-agnóstico)
│  ├─ entities.py          # Modelos del dominio (Item, Page, Document, Record, ContentType)
│  └─ ports.py             # Interfaces (HtmlFetcher, DocumentFetcher, HtmlParser, 
│                           #            TabularParser, PdfParser, Normalizer, OutputPort)
│
├─ application/            # Casos de uso (orquestan puertos)
│  ├─ use_cases.py         # Casos de uso base y genéricos
│  ├─ bcra_use_case.py      # BcraUseCase
│  ├─ bcra_monetario_use_case.py  # BcraMonetarioUseCase
│  ├─ parser_router.py     # ParserRouter (selecciona parser por ContentType)
│  ├─ universal_ingest_use_case.py  # UniversalIngestUseCase (múltiples formatos)
│  └─ tests/               # Tests de use cases
│
├─ adapters/               # Implementaciones (dependientes de frameworks)
│  ├─ fetchers/
│  │  ├─ http.py          # AdapterScrapyFetcher, AdapterScrapyDocumentFetcher
│  │  └─ scrapy.py        # AdapterHttpFetcher (stub para requests)
│  ├─ parsers/
│  │  ├─ bs4.py           # AdapterBs4Parser (stub, requiere beautifulsoup4)
│  │  ├─ bcra.py          # AdapterBcraParser (funciona)
│  │  ├─ bcra_excel.py    # AdapterBcraExcelParser (Excel BCRA, funciona)
│  │  ├─ csv.py           # AdapterCsvParser (stub básico)
│  │  ├─ excel.py         # AdapterExcelParser (stub, requiere openpyxl)
│  │  ├─ pdf.py           # AdapterPdfParser (stub, requiere pdfplumber)
│  │  ├─ registry.py      # PARSER_REGISTRY (registro centralizado)
│  │  └─ tests/           # Tests de parsers
│  ├─ normalizers/
│  │  ├─ bcra.py          # AdapterBcraNormalizer
│  │  ├─ bcra_monetario.py  # AdapterBcraMonetarioNormalizer (funciona)
│  │  ├─ generic.py       # AdapterGenericNormalizer (fallback)
│  │  └─ tests/           # Tests de normalizers
│  └─ outputs/
│     ├─ stdout.py        # AdapterStdoutOutput
│     └─ json.py          # AdapterJsonOutput
│
└─ interface/              # Entrada/Delivery (spiders, CLI)
   └─ spiders/
      ├─ bcra_spider.py      # Spider para BCRA HTML (funciona)
      ├─ bcra_monetario_spider.py  # Spider BCRA Excel (funciona)
      └─ universal_spider.py  # Spider genérico con ParserRouter (ejemplo)
```

### Patrón Puertos y Adaptadores

- **Puertos (Ports)**: Interfaces/Protocolos definidos en `core/ports.py` que representan contratos abstractos.
- **Adaptadores (Adapters)**: Implementaciones concretas en `adapters/` que implementan esos puertos usando frameworks específicos (Scrapy, BeautifulSoup, etc.).

Esto permite que la lógica de negocio (`application/`) permanezca independiente de frameworks externos.

### Soporte para Múltiples Formatos

El proyecto ahora soporta múltiples formatos de documentos:
- **HTML**: Parsing con BeautifulSoup4 (stub, requiere instalación)
- **CSV**: Parsing con módulo `csv` nativo (stub básico)
- **Excel (XLS/XLSX)**: Parsing con openpyxl/xlrd (stub, requiere instalación)
- **PDF**: Parsing con pdfplumber/tabula-py (stub, requiere instalación)

El **ParserRouter** selecciona automáticamente el parser correcto según el Content-Type del documento.

## 📚 Documentación

- 📖 [Arquitectura Escalable](docs/ARQUITECTURA_SCALABLE.md) - Guía completa de la arquitectura y cómo agregar nuevos sitios/formatos
- 🕷️ [Cómo Funciona Scrapy](docs/COMO_SCRAPY_FUNCIONA.md) - Explicación de cómo Scrapy pasa el response al spider
- 🔍 [Cómo Scrapy Busca Variables](docs/COMO_SCRAPY_BUSCA_VARIABLES.md) - Cómo Scrapy encuentra y usa las variables del spider
- 🎯 [Para Qué Sirve el Normalizer](docs/PARA_QUE_SIRVE_NORMALIZER.md) - Explicación del rol del Normalizer en la arquitectura

## 🚀 Instalación

### 1. Crear entorno virtual

```bash
python -m venv .venv
source .venv/bin/activate  # En Windows: .venv\Scripts\activate
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

## ▶️ Uso

### Ejecutar spiders

```bash
# Spider BCRA (HTML)
make crawl SPIDER=bcra
# o directamente:
scrapy crawl bcra

# Spider BCRA Monetario (Excel)
make crawl SPIDER=bcra_monetario
# o directamente:
scrapy crawl bcra_monetario

# Spider universal (múltiples formatos)
scrapy crawl universal -a url="https://example.com"
```

**Archivos de salida:**
- `bcra_data.json` - Datos de BCRA Principales Variables (HTML)
- `bcra_monetario_data.json` - Datos de BCRA Informe Monetario Diario (Excel)

### Ejecutar tests

```bash
# Todos los tests
make test

# Tests con cobertura
make test-cov

# O directamente con pytest
pytest ingestor_scrapper/ -v
```

**Cobertura actual:** 87% de los módulos principales.

### Ejemplo: Spider BCRA Monetario

El spider `bcra_monetario` muestra el patrón completo de Clean Architecture:

```python
from ingestor_scrapper.adapters.fetchers import AdapterScrapyDocumentFetcher
from ingestor_scrapper.adapters.parsers.bcra_excel import AdapterBcraExcelParser
from ingestor_scrapper.adapters.normalizers.bcra_monetario import AdapterBcraMonetarioNormalizer
from ingestor_scrapper.adapters.outputs import AdapterJsonOutput
from ingestor_scrapper.application.bcra_monetario_use_case import BcraMonetarioUseCase

class BcraMonetarioSpider(scrapy.Spider):
    name = "bcra_monetario"
    start_urls = ["https://www.bcra.gob.ar/..."]

    def parse_excel(self, response):
        fetcher = AdapterScrapyDocumentFetcher(response)
        parser = AdapterBcraExcelParser()
        normalizer = AdapterBcraMonetarioNormalizer()
        output = AdapterJsonOutput(output_file="bcra_monetario_data.json")
        
        use_case = BcraMonetarioUseCase(fetcher, parser, normalizer, output)
        items = use_case.execute(response.url)
```

### Agregar nuevos spiders

Para crear un nuevo spider, consulta: [Arquitectura Escalable](docs/ARQUITECTURA_SCALABLE.md)

## 📦 Estructura del Proyecto

- **`core/`**: Capa de dominio con entidades y puertos (interfaces). Framework-agnóstico.
- **`application/`**: Casos de uso que orquestan los puertos. Incluye tests.
- **`adapters/`**: Implementaciones concretas de los puertos. Incluye tests organizados por módulo.
- **`interface/`**: Puntos de entrada (spiders de Scrapy, futuros CLI, APIs, etc.).

## 🗺️ Roadmap

### Próximos pasos sugeridos:

1. **Implementar parsers de stubs**: Completar implementación de parsers para CSV, PDF
   - CSV: Está básico, expandir funcionalidad
   - PDF: Instalar pdfplumber y completar parser

2. **Pipelines de Scrapy**: Activar pipelines para procesamiento de items
   - Descomentar sección de pipelines en `settings.py`
   - Crear pipelines para validación, limpieza, almacenamiento

3. **Storage**: Agregar adaptadores de salida a archivos/base de datos
   - `AdapterDatabaseOutput` para persistir en DB
   - `AdapterApiOutput` para enviar a APIs

4. **Expandir tests**: Aumentar cobertura
   - Tests de fetchers, outputs
   - Tests de integración end-to-end

## 📝 Notas

- El proyecto sigue el patrón de **parser por proveedor/sitio** para máxima flexibilidad.
- Los tests están organizados junto a los módulos que testean.
- Para scrapear un nuevo sitio, consulta [Arquitectura Escalable](docs/ARQUITECTURA_SCALABLE.md).
- Todos los archivos incluyen **TODOs** donde se puede expandir la funcionalidad.

## 🧪 Testing

El proyecto incluye tests unitarios con 87% de cobertura:

```bash
# Ejecutar todos los tests
make test

# Ejecutar con cobertura detallada
make test-cov

# Linting automático
ruff check --fix ingestor_scrapper/ tests/
ruff format ingestor_scrapper/ tests/
```

Tests organizados por módulo:
- `adapters/parsers/tests/` - Tests de parsers (13 tests)
- `adapters/normalizers/tests/` - Tests de normalizers (9 tests)  
- `application/tests/` - Tests de use cases (8 tests)

## 📚 Referencias

- [Scrapy Documentation](https://docs.scrapy.org/)
- [Clean Architecture (Hexagonal Architecture)](https://alistair.cockburn.us/hexagonal-architecture/)
- [Ports & Adapters Pattern](https://herbertograca.com/2017/11/16/explicit-architecture-01-ddd-hexagonal-onion-clean-cqrs-how-i-put-it-all-together/)
