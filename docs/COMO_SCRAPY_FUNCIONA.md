# 🕷️ ¿Quién le pasa el `response` al Spider?

## 🎯 Respuesta Corta

**Scrapy es quien le pasa el `response` al spider**. Cuando ejecutas `scrapy crawl bcra`, el motor de Scrapy:
1. Lee tus `start_urls`
2. Hace las peticiones HTTP
3. Cuando recibe la respuesta, **automáticamente llama** a tu método `parse(response)`

---

## 📋 Flujo Completo: Cómo Scrapy Funciona Internamente

### **PASO 1: Ejecutas el Comando**

```bash
scrapy crawl bcra
```

### **PASO 2: Scrapy Carga tu Spider**

Scrapy busca el spider con `name = "bcra"` y lo instancia:

```python
# Scrapy internamente hace algo como esto:
spider = BcraSpider()
```

### **PASO 3: Scrapy Lee tus `start_urls`**

```python
# Tu spider define:
start_urls = ["https://www.bcra.gob.ar/..."]
```

Scrapy toma estas URLs y las convierte en **Request objects**:

```python
# Scrapy internamente crea:
requests = [
    Request(url="https://www.bcra.gob.ar/...")
]
```

### **PASO 4: Scrapy Engine Envía las Requests**

El **Scrapy Engine** (el motor interno) envía las requests al **Downloader**:

```
Spider → Engine → Scheduler → Downloader → Internet
```

### **PASO 5: Downloader Obtiene la Respuesta**

El **Downloader** hace la petición HTTP real y recibe la respuesta:

```
Internet → Downloader → Response object
```

El Response object contiene:
- `response.url` - La URL solicitada
- `response.text` - El HTML completo
- `response.status` - El código HTTP (200, 404, etc.)
- `response.headers` - Los headers HTTP

### **PASO 6: Scrapy Engine Llama a tu `parse()`**

Aquí está la magia: **Scrapy automáticamente llama** a tu método `parse()`:

```python
# Scrapy internamente hace algo como esto:
response = downloader.fetch(request)  # Obtiene el response
spider.parse(response)  # ✨ Llama automáticamente a tu método
```

Por eso **tú no llamas** a `parse()` directamente. Scrapy lo hace automáticamente.

---

## 🔍 Visualización del Flujo

```
┌─────────────────────────────────────────────────────────────┐
│  TÚ EJECUTAS: scrapy crawl bcra                             │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  SCRAPY ENGINE (Motor Interno)                               │
│  1. Carga tu spider: BcraSpider()                            │
│  2. Lee: start_urls = ["https://..."]                        │
│  3. Crea Request objects                                      │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  DOWNLOADER (Hace las Peticiones HTTP)                        │
│  1. Envía request a: https://www.bcra.gob.ar/...            │
│  2. Recibe respuesta HTTP                                    │
│  3. Crea Response object con:                                │
│     - response.url                                            │
│     - response.text (HTML)                                    │
│     - response.status                                         │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  SCRAPY ENGINE llama automáticamente:                       │
│                                                               │
│  spider.parse(response)  ← ✨ AQUÍ LE PASA EL RESPONSE       │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  TU CÓDIGO (bcra_spider.py)                                  │
│                                                               │
│  def parse(self, response: Response) -> None:                │
│      # response ya está aquí, Scrapy lo pasó                 │
│      fetcher = AdapterScrapyFetcher(response)                │
│      ...                                                      │
└─────────────────────────────────────────────────────────────┘
```

---

## 💡 Conceptos Clave

### **1. Convención de Nombres**

Scrapy usa **convención de nombres**:
- Si defines un método `parse()`, Scrapy lo llama automáticamente
- Si quieres usar otro nombre, puedes especificarlo en `Request`:

```python
def parse(self, response):
    # ...

# O con callback personalizado:
yield Request(url=url, callback=self.mi_metodo_personalizado)

def mi_metodo_personalizado(self, response):
    # Scrapy llamará a este método en lugar de parse()
    ...
```

### **2. Por qué `response` tiene todo**

Cuando Scrapy recibe la respuesta HTTP, la envuelve en un objeto `Response` que tiene:
- **`response.url`**: URL solicitada
- **`response.text`**: HTML completo como string
- **`response.body`**: Contenido raw en bytes
- **`response.status`**: Código HTTP (200, 404, 500, etc.)
- **`response.headers`**: Headers HTTP
- Y muchos más atributos útiles

### **3. El Engine es el "Cerebro"**

El **Scrapy Engine** coordina todo:
- Recibe requests del spider
- Las envía al downloader
- Recibe responses del downloader
- Llama a los callbacks (como `parse()`) del spider
- Maneja la cola de requests
- Controla la concurrencia

---

## 🎓 Resumen

| Pregunta | Respuesta |
|----------|-----------|
| **¿Quién pasa el response?** | Scrapy Engine (el motor interno) |
| **¿Cuándo lo pasa?** | Automáticamente cuando recibe la respuesta HTTP |
| **¿Cómo lo pasa?** | Llama a tu método `parse(response)` |
| **¿Por qué no lo llamas tú?** | Scrapy maneja todo el ciclo de vida de las requests/responses |
| **¿Qué contiene el response?** | URL, HTML, status code, headers, y más |

---

## 🔗 Documentación Oficial

Para entender más sobre el flujo interno de Scrapy:

- [Scrapy Architecture](https://docs.scrapy.org/en/latest/topics/architecture.html)
- [How Scrapy Works](https://docs.scrapy.org/en/latest/topics/architecture.html#data-flow)
- [Request/Response Flow](https://docs.scrapy.org/en/latest/topics/request-response.html)

---

## 💭 Analogía Simple

Imagina que Scrapy es un **asistente personal**:

1. Tú le dices: "Scrapy, scrapea estas URLs" (`scrapy crawl bcra`)
2. Scrapy lee tu lista de URLs (`start_urls`)
3. Scrapy va a internet y obtiene las páginas
4. Scrapy te trae el HTML en un paquete (`response`)
5. Scrapy te llama: "¡Listo! Aquí está el `response`, procesa lo que quieras"

**Tú nunca llamas directamente** a `parse()`. Scrapy lo hace por ti cuando tiene el response listo.

