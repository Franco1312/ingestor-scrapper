# 🏗️ Arquitectura Escalable - ingestor-scrapper-v2

## 📋 Descripción General

Este documento explica la arquitectura escalable del proyecto, diseñada para soportar múltiples sitios y formatos (HTML, CSV, Excel, PDF) manteniendo Clean Architecture (Ports & Adapters).

## 🎯 Objetivos de la Arquitectura

1. **Mantenibilidad**: Separación clara de responsabilidades
2. **Escalabilidad**: Fácil agregar nuevos sitios y formatos
3. **Flexibilidad**: Soporte para múltiples formatos (HTML, CSV, Excel, PDF)
4. **Compatibilidad**: Los spiders existentes siguen funcionando

## 📐 Diagrama de Flujo

```
┌─────────────────────────────────────────────────────────────┐
│                     SPIDER                                   │
│  (bcra_spider, scrapethissite_spider, universal_spider)    │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                  USE CASE                                    │
│  (BcraUseCase, ScrapeThisSiteUseCase, UniversalIngestUseCase)│
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              DOCUMENT FETCHER                                │
│  (AdapterScrapyDocumentFetcher)                             │
│  - Detecta Content-Type (HTML, CSV, Excel, PDF)             │
│  - Crea Document entity                                      │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              PARSER ROUTER                                   │
│  (ParserRouter)                                              │
│  - Selecciona parser según ContentType                      │
│  - Usa PARSER_REGISTRY                                      │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                  PARSER                                      │
│  - HtmlParser (AdapterBs4Parser)                            │
│  - TabularParser (AdapterCsvParser, AdapterExcelParser)      │
│  - PdfParser (AdapterPdfParser)                             │
│  - Convierte Document → List[Record]                        │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                NORMALIZER                                    │
│  (AdapterBcraNormalizer, AdapterGenericNormalizer)          │
│  - Convierte List[Record] → List[Item]                      │
│  - Mapea campos específicos del sitio                       │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                 OUTPUT PORT                                  │
│  (AdapterStdoutOutput, AdapterJsonOutput)                   │
│  - Emite List[Item]                                          │
└─────────────────────────────────────────────────────────────┘
```

## 🧩 Componentes Principales

### 1. Core (Dominio)

#### Entities

- **`Item`**: Entidad del dominio para items extraídos (title, content, url)
- **`Page`**: Entidad para páginas HTML (mantiene compatibilidad)
- **`Document`**: Entidad genérica para documentos (soporta múltiples formatos)
- **`Record`**: Entidad intermedia del parser (data dict, source_url, fetched_at)
- **`ContentType`**: Enum para tipos de contenido (HTML, CSV, XLS, XLSX, PDF, UNKNOWN)

#### Ports

- **`HtmlFetcher`**: Puerto para obtener HTML (compatibilidad)
- **`DocumentFetcher`**: Puerto genérico para obtener documentos
- **`HtmlParser`**: Puerto para parsear HTML → Records
- **`TabularParser`**: Puerto para parsear CSV/Excel → Records
- **`PdfParser`**: Puerto para parsear PDF → Records
- **`Normalizer`**: Puerto para normalizar Records → Items
- **`OutputPort`**: Puerto para emitir Items (mantiene compatibilidad)

### 2. Application (Casos de Uso)

- **`CrawlAndParseUseCase`**: Caso de uso genérico (compatibilidad)
- **`BcraUseCase`**: Caso de uso específico para BCRA
- **`ScrapeThisSiteUseCase`**: Caso de uso específico para ScrapeThisSite
- **`UniversalIngestUseCase`**: Caso de uso genérico con ParserRouter
- **`ParserRouter`**: Componente que selecciona parser según ContentType

### 3. Adapters (Implementaciones)

#### Fetchers

- **`AdapterScrapyFetcher`**: Implementa HtmlFetcher (compatibilidad)
- **`AdapterScrapyDocumentFetcher`**: Implementa DocumentFetcher, detecta ContentType
- **`AdapterHttpFetcher`**: Stub para requests (no Scrapy)

#### Parsers

- **`AdapterBs4Parser`**: Parser HTML genérico (stub, requiere beautifulsoup4)
- **`AdapterBcraParser`**: Parser HTML específico para BCRA (existente, funciona)
- **`AdapterScrapeThisSiteParser`**: Parser HTML específico (existente, funciona)
- **`AdapterCsvParser`**: Parser CSV (stub básico)
- **`AdapterExcelParser`**: Parser Excel (stub, requiere openpyxl/xlrd)
- **`AdapterPdfParser`**: Parser PDF (stub, requiere pdfplumber)

#### Normalizers

- **`AdapterBcraNormalizer`**: Normaliza Records de BCRA → Items
- **`AdapterGenericNormalizer`**: Normaliza Records genéricos → Items (fallback)

#### Outputs

- **`AdapterStdoutOutput`**: Emite Items a consola
- **`AdapterJsonOutput`**: Emite Items como JSON a archivo

#### Registry

- **`PARSER_REGISTRY`**: Registro centralizado de parsers por ContentType

### 4. Interface (Spiders)

- **`BcraSpider`**: Spider específico para BCRA (funciona)
- **`ScrapeThisSiteSpider`**: Spider específico para ScrapeThisSite (funciona)
- **`UniversalSpider`**: Spider genérico que usa UniversalIngestUseCase

## 📝 Cómo Agregar un Nuevo Sitio

### Paso 1: Crear Parser (si es necesario)

Si el sitio requiere parsing específico, crea un nuevo parser que implemente `HtmlParser`:

```python
# adapters/parser_mi_sitio.py
from ingestor_scrapper.core.entities import Document, Record
from ingestor_scrapper.core.ports import HtmlParser

class AdapterMiSitioParser(HtmlParser):
    def parse(self, document: Document) -> List[Record]:
        # Lógica de parsing específica
        records = []
        # ... extraer datos ...
        return records
```

### Paso 2: Crear Normalizer (si es necesario)

Si el sitio requiere normalización específica:

```python
# adapters/normalizer_mi_sitio.py
from ingestor_scrapper.core.entities import Item, Record
from ingestor_scrapper.core.ports import Normalizer

class AdapterMiSitioNormalizer(Normalizer):
    def normalize(self, records: List[Record]) -> List[Item]:
        items = []
        # ... mapear Records a Items ...
        return items
```

### Paso 3: Crear Use Case (opcional)

Si necesitas lógica específica del sitio:

```python
# application/mi_sitio_use_case.py
from ingestor_scrapper.application.use_cases import UseCase
from ingestor_scrapper.core.ports import DocumentFetcher, Normalizer, OutputPort
from ingestor_scrapper.core.ports import HtmlParser

class MiSitioUseCase(UseCase):
    def __init__(self, fetcher, parser, normalizer, output):
        # ...
```

### Paso 4: Crear Spider

```python
# interface/spiders/mi_sitio_spider.py
class MiSitioSpider(scrapy.Spider):
    name = "mi_sitio"
    start_urls = ["https://mi-sitio.com"]
    
    def parse(self, response):
        fetcher = AdapterScrapyDocumentFetcher(response)
        parser = AdapterMiSitioParser()
        normalizer = AdapterMiSitioNormalizer()
        output = AdapterJsonOutput()
        
        use_case = MiSitioUseCase(fetcher, parser, normalizer, output)
        use_case.execute(response.url)
```

## 📊 Cómo Agregar un Nuevo Formato

### Paso 1: Implementar el Parser Port

Si el formato no existe, primero crea el Port (si es necesario):

```python
# core/ports.py
class MiFormatoParser(ABC):
    @abstractmethod
    def parse(self, document: Document) -> List[Record]:
        pass
```

### Paso 2: Crear el Adapter

```python
# adapters/parser_mi_formato.py
from ingestor_scrapper.core.entities import Document, Record
from ingestor_scrapper.core.ports import MiFormatoParser

class AdapterMiFormatoParser(MiFormatoParser):
    def parse(self, document: Document) -> List[Record]:
        # TODO: Instalar librería necesaria
        # pip install mi-libreria
        records = []
        # ... parsear documento ...
        return records
```

### Paso 3: Agregar al Registry

```python
# adapters/parsers/registry.py
from ingestor_scrapper.adapters.parsers.mi_formato import (
    AdapterMiFormatoParser,
)
from ingestor_scrapper.core.entities import ContentType

_MI_FORMATO_PARSER = AdapterMiFormatoParser()

PARSER_REGISTRY = {
    # ... existentes ...
    ContentType.MI_FORMATO: _MI_FORMATO_PARSER,
}
```

### Paso 4: Agregar ContentType (si es necesario)

```python
# core/entities.py
class ContentType(Enum):
    # ... existentes ...
    MI_FORMATO = "mi_formato"
```

## ✅ Checklist para Nuevos Parsers

- [ ] Detectar Content-Type (en DocumentFetcher o Parser)
- [ ] Crear Adapter del Port correspondiente
- [ ] Implementar método `parse(document: Document) -> List[Record]`
- [ ] Agregar al `PARSER_REGISTRY`
- [ ] (Opcional) Crear Normalizer específico
- [ ] Probar con `UniversalSpider`
- [ ] Documentar dependencias requeridas (TODO en código)

## 🔧 Dependencias Futuras

Las siguientes librerías se pueden agregar cuando se implementen los parsers:

### HTML Parsing
```bash
pip install beautifulsoup4 lxml
```
- **beautifulsoup4**: Para parsing HTML genérico
- **lxml**: Parser más rápido (opcional)

### Excel Parsing
```bash
pip install openpyxl
# Para XLS legacy:
pip install xlrd  # Nota: usar versión < 2.0 para XLS
```
- **openpyxl**: Para archivos .xlsx
- **xlrd**: Para archivos .xls (legacy)

### PDF Parsing
```bash
pip install pdfplumber
# Alternativa:
pip install tabula-py  # Requiere Java
```
- **pdfplumber**: Recomendado (puro Python)
- **tabula-py**: Alternativa (requiere Java)

### HTTP Requests (alternativa a Scrapy)
```bash
pip install requests
```
- **requests**: Para fetching sin Scrapy (scripts standalone)

## 🎓 Ejemplos de Uso

### Ejemplo 1: Spider Existente (BCRA)

```python
# BcraSpider sigue funcionando igual
scrapy crawl bcra
```

### Ejemplo 2: Universal Spider

```python
# Maneja múltiples formatos automáticamente
scrapy crawl universal -a url="https://example.com/data.html"
scrapy crawl universal -a url="https://example.com/data.csv"
scrapy crawl universal -a url="https://example.com/data.xlsx"
```

### Ejemplo 3: Nuevo Sitio con Parsing Específico

```python
# 1. Crear parser específico
class AdapterNuevoSitioParser(HtmlParser):
    def parse(self, document: Document) -> List[Record]:
        # Parsing específico
        pass

# 2. Crear spider
class NuevoSitioSpider(scrapy.Spider):
    name = "nuevo_sitio"
    # ... usar UniversalIngestUseCase o UseCase específico ...
```

## 🔍 Notas Importantes

1. **Compatibilidad**: Los spiders existentes (`bcra`, `scrapethissite`) siguen funcionando
2. **Stubs**: Muchos parsers son stubs con TODOs - no están completamente implementados
3. **Normalizers**: Use normalizers específicos cuando sea posible; `AdapterGenericNormalizer` es fallback
4. **Content-Type Detection**: Se hace automáticamente en `AdapterScrapyDocumentFetcher`
5. **ParserRouter**: Selecciona automáticamente el parser correcto según ContentType

## 📚 Archivos Clave

- **`core/entities.py`**: Value Objects (Document, Record, ContentType)
- **`core/ports.py`**: Interfaces (DocumentFetcher, HtmlParser, TabularParser, etc.)
- **`application/parser_router.py`**: Router que selecciona parsers
- **`application/universal_ingest_use_case.py`**: Use case genérico
- **`adapters/registry.py`**: Registro centralizado de parsers
- **`interface/spiders/universal_spider.py`**: Spider de ejemplo

## 🚀 Próximos Pasos

1. Implementar parsers reales (BeautifulSoup4, openpyxl, pdfplumber)
2. Agregar más normalizers específicos por sitio
3. Mejorar detección de Content-Type
4. Agregar tests unitarios
5. Documentar casos de uso específicos

