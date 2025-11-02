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
│  ├─ parser_router.py     # ParserRouter (selecciona parser por ContentType)
│  └─ universal_ingest_use_case.py  # UniversalIngestUseCase (múltiples formatos)
│
├─ adapters/               # Implementaciones (dependientes de frameworks)
│  ├─ fetcher_scrapy.py    # AdapterScrapyFetcher, AdapterScrapyDocumentFetcher
│  ├─ fetcher_http.py      # AdapterHttpFetcher (stub para requests)
│  ├─ parser_bs4.py       # AdapterBs4Parser (stub, requiere beautifulsoup4)
│  ├─ parser_bcra.py       # AdapterBcraParser (funciona)
│  ├─ parser_csv.py        # AdapterCsvParser (stub básico)
│  ├─ parser_excel.py      # AdapterExcelParser (stub, requiere openpyxl)
│  ├─ parser_pdf.py        # AdapterPdfParser (stub, requiere pdfplumber)
│  ├─ normalizer_bcra.py   # AdapterBcraNormalizer
│  ├─ normalizer_generic.py  # AdapterGenericNormalizer (fallback)
│  ├─ output_stdout.py     # AdapterStdoutOutput
│  ├─ output_json.py       # AdapterJsonOutput
│  └─ registry.py          # PARSER_REGISTRY (registro centralizado)
│
└─ interface/              # Entrada/Delivery (spiders, CLI)
   └─ spiders/
      ├─ bcra_spider.py     # Spider para BCRA (funciona)
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

### Ejecutar el spider de BCRA

```bash
scrapy crawl bcra
```

Este comando ejecutará el `bcra_spider` que extrae datos financieros de `https://www.bcra.gob.ar/PublicacionesEstadisticas/Principales_variables.asp` y genera un archivo JSON (`bcra_data.json`) con los resultados estructurados.

### Ejecutar el spider universal

```bash
scrapy crawl universal -a url="https://example.com"
```

Este spider puede manejar múltiples formatos (HTML, CSV, Excel, PDF) automáticamente usando el ParserRouter.

### Crear un nuevo spider

Para crear un nuevo spider, consulta la documentación: [Arquitectura Escalable](docs/ARQUITECTURA_SCALABLE.md)

Ejemplo básico:

```python
from ingestor_scrapper.adapters.fetchers import AdapterScrapyDocumentFetcher
from ingestor_scrapper.adapters.parsers import AdapterBcraParser
from ingestor_scrapper.adapters.normalizers import AdapterBcraNormalizer
from ingestor_scrapper.adapters.outputs import AdapterJsonOutput
from ingestor_scrapper.application.bcra_use_case import BcraUseCase

class MiSpider(scrapy.Spider):
    name = "mi_spider"
    start_urls = ["https://example.com"]
    
    def parse(self, response):
        fetcher = AdapterScrapyDocumentFetcher(response)
        parser = AdapterBcraParser()  # O tu parser específico
        normalizer = AdapterBcraNormalizer()  # O tu normalizer específico
        output = AdapterJsonOutput()
        
        use_case = BcraUseCase(fetcher, parser, normalizer, output)
        items = use_case.execute(response.url)
```

## 📦 Estructura del Proyecto

- **`core/`**: Capa de dominio con entidades y puertos (interfaces). Framework-agnóstico.
- **`application/`**: Casos de uso que orquestan los puertos para cumplir requisitos de negocio.
- **`adapters/`**: Implementaciones concretas de los puertos usando frameworks externos (Scrapy, BeautifulSoup, etc.).
- **`interface/`**: Puntos de entrada (spiders de Scrapy, futuros CLI, APIs, etc.).

## 🗺️ Roadmap

### Próximos pasos sugeridos:

1. **Agregar nuevos parsers**: Crear parsers específicos para nuevos sitios
   - Ver [Arquitectura Escalable](docs/ARQUITECTURA_SCALABLE.md) para guía completa

2. **Implementar parsers de stubs**: Completar implementación de parsers para CSV, Excel, PDF
   - Instalar dependencias necesarias (beautifulsoup4, openpyxl, pdfplumber)
   - Implementar lógica de parsing en los stubs

3. **Pipelines de Scrapy**: Activar pipelines para procesamiento de items
   - Descomentar sección de pipelines en `settings.py`
   - Crear pipelines para validación, limpieza, almacenamiento

4. **Storage**: Agregar adaptadores de salida a archivos/base de datos
   - `AdapterDatabaseOutput` para persistir en DB
   - `AdapterApiOutput` para enviar a APIs

5. **Tests**: Agregar tests unitarios para cada capa
   - Tests de use cases
   - Tests de adaptadores (mocks)
   - Tests de integración

## 📝 Notas

- El proyecto sigue el patrón de **parser por proveedor/sitio** para máxima flexibilidad.
- Para scrapear un nuevo sitio, consulta [Arquitectura Escalable](docs/ARQUITECTURA_SCALABLE.md).
- Todos los archivos incluyen **TODOs** donde se puede expandir la funcionalidad.

## 📚 Referencias

- [Scrapy Documentation](https://docs.scrapy.org/)
- [Clean Architecture (Hexagonal Architecture)](https://alistair.cockburn.us/hexagonal-architecture/)
- [Ports & Adapters Pattern](https://herbertograca.com/2017/11/16/explicit-architecture-01-ddd-hexagonal-onion-clean-cqrs-how-i-put-it-all-together/)
