# 🕷️ Cómo Scrapear tu Primera Página - Guía Paso a Paso

## 🎯 ¿Qué hace el `example_spider`?

El spider `example_spider.py` scrapea `https://example.com` y extrae el título de la página.

## 📋 Flujo Completo (Paso a Paso)

### **FASE 1: Scrapy Inicia el Spider**

1. **Ejecutas el comando:**
   ```bash
   scrapy crawl example
   ```

2. **Scrapy lee el spider:**
   - Ve que `name = "example"`
   - Lee `start_urls = ["https://example.com"]`
   - Hace una petición HTTP a esa URL
   - Cuando recibe la respuesta, llama automáticamente a `parse(response)`

---

### **FASE 2: El método `parse()` se ejecuta**

El método `parse()` es donde ocurre toda la magia. Veamos qué hace:

```python
def parse(self, response):
    # response es un objeto de Scrapy que contiene:
    # - response.url = "https://example.com"
    # - response.text = "<html>...</html>" (HTML completo)
    # - response.status = 200 (código HTTP)
```

**Paso 2.1: Crear los adapters (líneas 62-65)**
```python
# Crea 3 "adaptadores" que convierten Scrapy → nuestro dominio
fetcher = AdapterScrapyFetcher(response)  # Convierte Response → Page
parser = AdapterBs4Parser()              # Convierte HTML → Items
output = AdapterStdoutOutput()           # Imprime los items en consola
```

**Paso 2.2: Crear el caso de uso (líneas 67-70)**
```python
# Crea el "orquestador" que coordina todo el flujo
use_case = CrawlAndParseUseCase(
    fetcher=fetcher,  # Le pasamos el fetcher
    parser=parser,    # Le pasamos el parser
    output=output     # Le pasamos el output
)
```

**Paso 2.3: Ejecutar el caso de uso (línea 76)**
```python
items = use_case.execute(response.url)  # ¡Aquí comienza el scraping!
```

---

### **FASE 3: El Use Case Orquesta el Flujo**

Cuando ejecutas `use_case.execute(url)`, esto es lo que pasa **dentro**:

**Paso 3.1: Fetch (Obtener HTML) - Línea 68**
```python
page: Page = self.fetcher.fetch(url)
```
- Llama a `AdapterScrapyFetcher.fetch()`
- Este adapter toma `response.text` (el HTML) y crea un objeto `Page`
- **Resultado:** `Page(url="https://example.com", html="<html>...</html>", status_code=200)`

**Paso 3.2: Parse (Extraer Datos) - Línea 74**
```python
items: List[Item] = self.parser.parse(page.html, page.url)
```
- Llama a `AdapterBs4Parser.parse()` con el HTML
- Este adapter busca `<title>` y `<h1>` en el HTML
- **Resultado:** `[Item(title="Example Domain", content="...", url="https://example.com")]`

**Paso 3.3: Output (Mostrar Resultados) - Línea 79**
```python
self.output.emit(items)
```
- Llama a `AdapterStdoutOutput.emit()` con los items
- Imprime en consola: `Found 1 item(s): [1] Title: Example Domain`

---

### **FASE 4: Devolver Resultados**

El `use_case.execute()` devuelve la lista de `items`, y el spider los loguea:

```python
logger.info("Successfully parsed %s", response.url)
logger.info("Extracted title: %s", title)
```

---

## 🔍 Flujo Visual Simplificado

```
1. Scrapy llama a parse(response)
   │
   ├─ 2. Crear adapters
   │  ├─ fetcher = AdapterScrapyFetcher(response)
   │  ├─ parser = AdapterBs4Parser()
   │  └─ output = AdapterStdoutOutput()
   │
   ├─ 3. Crear use case
   │  └─ use_case = CrawlAndParseUseCase(fetcher, parser, output)
   │
   ├─ 4. Ejecutar use case
   │  │
   │  ├─ 4.1 FETCH: fetcher.fetch() → Page
   │  │         (Response → Page entity)
   │  │
   │  ├─ 4.2 PARSE: parser.parse() → List[Item]
   │  │         (HTML → Items estructurados)
   │  │
   │  └─ 4.3 OUTPUT: output.emit() → Imprime
   │            (Items → Consola)
   │
   └─ 5. Recibir items y loguear
      └─ items = [Item(title="Example Domain", ...)]
```

---

## 🚀 Cómo Ejecutarlo

```bash
# 1. Activar entorno virtual
source .venv/bin/activate

# 2. Ejecutar el spider
scrapy crawl example
```

**Salida esperada:**
```
INFO: Found 1 item(s):
INFO:   [1] Title: Example Domain
INFO:      Content: Example Domain
INFO:      URL: https://example.com
INFO: Successfully parsed https://example.com
INFO: Extracted title: Example Domain
```

---

## 🔧 Cómo Modificarlo para Otra Página

### Opción 1: Modificar el spider existente

```python
class ExampleSpider(scrapy.Spider):
    name = "example"
    allowed_domains = ["tusitio.com"]  # Cambia el dominio
    start_urls = ["https://tusitio.com"]  # Cambia la URL
```

### Opción 2: Crear un nuevo spider

```bash
scrapy genspider mi_spider tusitio.com
```

Luego edita `mi_spider.py` para seguir el mismo patrón que `example_spider`.

---

## 💡 Conceptos Clave

1. **Spider (`example_spider.py`):** Punto de entrada. Scrapy lo ejecuta.
2. **Parse method:** Se ejecuta automáticamente cuando Scrapy recibe una respuesta.
3. **Adapters:** Convierten frameworks (Scrapy) → nuestro dominio (Page, Item).
4. **Use Case:** Orquesta el flujo: fetch → parse → output.
5. **Entities:** Modelos de datos simples (Page, Item).

---

## ❓ Preguntas Frecuentes

**P: ¿Por qué no scrapea directamente en el `parse()`?**
R: Por separación de responsabilidades. El `parse()` solo coordina; la lógica está en el use case.

**P: ¿Puedo scrapear múltiples URLs?**
R: Sí, puedes agregar más URLs a `start_urls` o hacer `yield scrapy.Request(url, callback=self.parse)`.

**P: ¿Cómo extraigo más datos?**
R: Modifica `AdapterBs4Parser.parse()` para buscar más elementos HTML.

