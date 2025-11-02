# 🔍 ¿Cómo Scrapy Busca las Variables del Spider?

## ✅ Respuesta Corta

**Sí, Scrapy busca esas variables por nombre** usando **reflexión/introspección** de Python. Cuando ejecutas `scrapy crawl bcra`, Scrapy:

1. Busca el spider con `name = "bcra"`
2. Lee `start_urls` para saber qué URLs scrapear
3. Lee `allowed_domains` para validar dominios
4. Usa estos valores automáticamente

---

## 🔎 Paso a Paso: Cómo Scrapy Encuentra tu Spider

### **PASO 1: Ejecutas el Comando**

```bash
scrapy crawl bcra
```

### **PASO 2: Scrapy Lee `scrapy.cfg` y `settings.py`**

```python
# settings.py
SPIDER_MODULES = ["ingestor_scrapper.interface.spiders"]
```

Esto le dice a Scrapy **dónde buscar** los spiders.

### **PASO 3: Scrapy Busca TODAS las Clases que Heredan de `scrapy.Spider`**

Scrapy escanea el directorio `ingestor_scrapper/interface/spiders/` y busca:

```python
# Scrapy internamente hace algo como esto:
import importlib
import inspect

# 1. Importa todos los módulos del directorio spiders
modules = importlib.import_module("ingestor_scrapper.interface.spiders")

# 2. Busca todas las clases que heredan de scrapy.Spider
for name, obj in inspect.getmembers(modules):
    if inspect.isclass(obj) and issubclass(obj, scrapy.Spider):
        # ¡Encontró un spider!
        spider_class = obj
```

### **PASO 4: Scrapy Lee el Atributo `name`**

```python
# Tu código:
class BcraSpider(scrapy.Spider):
    name = "bcra"  # ← Scrapy lee esto
```

Scrapy compara:

```python
# Scrapy internamente hace:
spider_name = BcraSpider.name  # Lee "bcra"
if spider_name == "bcra":  # ← "bcra" es lo que pediste en el comando
    # ¡Este es el spider correcto!
    spider = BcraSpider()
```

### **PASO 5: Scrapy Lee `start_urls`**

```python
# Tu código:
start_urls = [BCRA_PRINCIPALES_VARIABLES_URL]  # ← Scrapy lee esto
```

Scrapy internamente hace:

```python
# Scrapy internamente hace:
urls = spider.start_urls  # Lee ["https://www.bcra.gob.ar/..."]
for url in urls:
    request = Request(url=url, callback=spider.parse)
    # Agrega las requests a la cola
```

### **PASO 6: Scrapy Lee `allowed_domains` (si existe)**

```python
# Tu código:
allowed_domains = BCRA_DOMAINS  # ← Scrapy lee esto
```

Scrapy usa esto para **validar** que solo scrapees dominios permitidos:

```python
# Scrapy internamente valida:
if request.url not in spider.allowed_domains:
    # Rechaza la request si el dominio no está permitido
    raise Exception(f"Domain not allowed: {request.url}")
```

---

## 🎯 Visualización: Cómo Scrapy Busca los Atributos

```
┌─────────────────────────────────────────────────────────────┐
│  TÚ EJECUTAS: scrapy crawl bcra                              │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  SCRAPY: Lee settings.py                                     │
│  SPIDER_MODULES = ["ingestor_scrapper.interface.spiders"]    │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  SCRAPY: Escanea el directorio spiders/                     │
│  Busca todas las clases que heredan de scrapy.Spider        │
│                                                               │
│  Encuentra:                                                  │
│  - BcraSpider(scrapy.Spider)                                │
│  - ScrapeThisSiteSpider(scrapy.Spider)                      │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  SCRAPY: Lee atributos de clase usando reflexión            │
│                                                               │
│  Para cada spider:                                           │
│    spider.name = ???  ← Busca este atributo                  │
│                                                               │
│  BcraSpider.name = "bcra"  ← ✨ ¡Encontró el correcto!       │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  SCRAPY: Lee otros atributos de clase                        │
│                                                               │
│  spider.start_urls = ???      ← Lee start_urls               │
│  spider.allowed_domains = ??? ← Lee allowed_domains         │
│                                                               │
│  BcraSpider.start_urls = ["https://..."]                     │
│  BcraSpider.allowed_domains = ["bcra.gob.ar", ...]          │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  SCRAPY: Usa estos valores para iniciar el scraping          │
│                                                               │
│  for url in spider.start_urls:                               │
│      Request(url=url, callback=spider.parse)                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 💻 Cómo Funciona la Reflexión en Python

Scrapy usa **reflexión/introspección** de Python para leer atributos de clase:

```python
# Ejemplo simplificado de cómo Scrapy lo hace:

class BcraSpider(scrapy.Spider):
    name = "bcra"
    start_urls = ["https://www.bcra.gob.ar/..."]

# Scrapy internamente:
spider_class = BcraSpider

# Lee atributos de clase:
spider_name = spider_class.name  # "bcra"
start_urls = spider_class.start_urls  # ["https://..."]
allowed_domains = getattr(spider_class, "allowed_domains", None)  # Lee si existe
```

### **Atributos de Clase vs Instancia**

```python
class BcraSpider(scrapy.Spider):
    # ✅ Atributo de CLASE (lo que Scrapy busca)
    name = "bcra"
    start_urls = ["https://..."]
    
    def __init__(self):
        # ❌ Atributo de INSTANCIA (Scrapy NO lo busca así)
        self.custom_value = "something"
```

**Scrapy busca atributos de clase** porque necesita leerlos **antes** de crear la instancia.

---

## 📋 Variables que Scrapy Busca Automáticamente

| Variable | Propósito | ¿Obligatorio? | Ejemplo |
|----------|-----------|-----------------|---------|
| **`name`** | Identificador único del spider | ✅ **SÍ** | `name = "bcra"` |
| **`start_urls`** | URLs iniciales para scrapear | ✅ **SÍ** | `start_urls = ["https://..."]` |
| **`allowed_domains`** | Dominios permitidos | ⚠️ Opcional | `allowed_domains = ["bcra.gob.ar"]` |
| **`custom_settings`** | Configuración específica del spider | ⚠️ Opcional | `custom_settings = {...}` |

### **Variables que Scrapy NO busca (opcionales)**

```python
class BcraSpider(scrapy.Spider):
    name = "bcra"
    start_urls = ["https://..."]
    
    # Estas son opcionales, Scrapy las ignora si no existen:
    allowed_domains = [...]  # Opcional
    custom_settings = {...}  # Opcional
    
    # Estas son métodos que TÚ defines:
    def parse(self, response):  # Scrapy llama este método
        ...
```

---

## 🔧 Cómo Funciona `scrapy crawl`

Cuando ejecutas `scrapy crawl bcra`, internamente Scrapy hace:

```python
# Pseudocódigo de cómo Scrapy funciona:

def crawl(spider_name):
    # 1. Encuentra el spider por nombre
    spider_class = find_spider_by_name(spider_name)  # Busca name = "bcra"
    
    # 2. Crea instancia del spider
    spider = spider_class()
    
    # 3. Lee start_urls de la clase (no de la instancia)
    urls = spider_class.start_urls
    
    # 4. Crea requests para cada URL
    for url in urls:
        request = Request(url=url, callback=spider.parse)
        scheduler.add(request)
    
    # 5. Inicia el engine
    engine.start()
```

---

## 🎓 Resumen

| Pregunta | Respuesta |
|----------|-----------|
| **¿Scrapy busca las variables?** | ✅ Sí, usando reflexión/introspección |
| **¿Cómo las busca?** | Lee atributos de clase (ej: `Spider.name`) |
| **¿Cuándo las busca?** | Antes de crear la instancia del spider |
| **¿Dónde las busca?** | En el directorio `SPIDER_MODULES` |
| **¿Qué busca?** | Clases que heredan de `scrapy.Spider` |
| **¿Qué atributos lee?** | `name`, `start_urls`, `allowed_domains`, etc. |

---

## 💡 Conceptos Clave

### **1. Convención sobre Configuración**

Scrapy usa **convención sobre configuración**:
- Si defines `name = "bcra"`, Scrapy lo encuentra automáticamente
- No necesitas registrar el spider en ningún archivo de configuración
- Scrapy usa **nombres de atributos específicos** (`name`, `start_urls`)

### **2. Atributos de Clase**

```python
# ✅ Scrapy busca esto (atributo de clase):
class BcraSpider(scrapy.Spider):
    name = "bcra"  # ← Atributo de CLASE

# ❌ Scrapy NO busca esto (atributo de instancia):
def __init__(self):
    self.name = "bcra"  # ← Atributo de INSTANCIA
```

### **3. Herencia de `scrapy.Spider`**

Scrapy solo busca clases que heredan de `scrapy.Spider`:

```python
# ✅ Scrapy lo encuentra:
class BcraSpider(scrapy.Spider):
    name = "bcra"

# ❌ Scrapy lo ignora:
class MiClase:  # No hereda de scrapy.Spider
    name = "bcra"
```

---

## 🚀 Ejemplo Práctico

Imagina que tienes dos spiders:

```python
# bcra_spider.py
class BcraSpider(scrapy.Spider):
    name = "bcra"
    start_urls = ["https://www.bcra.gob.ar/..."]

# scrapethissite_spider.py
class ScrapeThisSiteSpider(scrapy.Spider):
    name = "scrapethissite"
    start_urls = ["https://www.scrapethissite.com/..."]
```

Cuando ejecutas `scrapy crawl bcra`:

1. Scrapy busca en `ingestor_scrapper/interface/spiders/`
2. Encuentra `BcraSpider` y `ScrapeThisSiteSpider`
3. Lee `BcraSpider.name = "bcra"` ✅
4. Lee `ScrapeThisSiteSpider.name = "scrapethissite"` (lo ignora)
5. Usa `BcraSpider.start_urls` para iniciar el scraping

---

## 📚 Documentación

Para más información sobre cómo Scrapy busca spiders:

- [Scrapy Spider Discovery](https://docs.scrapy.org/en/latest/topics/spiders.html#spider-discovery)
- [Spider Attributes](https://docs.scrapy.org/en/latest/topics/spiders.html#spider-attributes)

