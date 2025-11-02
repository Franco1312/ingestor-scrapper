# 🎯 ¿Para qué sirve el Normalizer?

## 📋 Función Principal

El **Normalizer** es una capa de transformación que convierte **Records genéricos** (salida del Parser) en **Items estructurados** (formato del dominio).

## 🔄 Flujo Completo

```
1. Parser extrae datos → List[Record]
   └─ Record tiene: data={detalle: "X", fecha: "29/10/2025", valor: "40.764"}
   
2. Normalizer transforma → List[Item]
   └─ Item tiene: title="X", content={fecha: "2025-10-29", valor: 40764.0}, url="..."
   
3. Output emite → JSON/Archivo
```

## 🎯 ¿Por qué separar Parser y Normalizer?

### **1. Separación de Responsabilidades**

- **Parser**: Solo extrae datos del HTML/documento (responsabilidad única)
- **Normalizer**: Solo transforma datos a formato del dominio (responsabilidad única)

### **2. Flexibilidad**

El mismo **Parser** puede usarse con diferentes **Normalizers** según el caso:

```python
# Mismo parser BCRA...
parser = AdapterBcraParser()

# ... pero diferentes normalizers según necesites:
normalizer1 = AdapterBcraNormalizer()      # Formato estructurado (fecha ISO, valor number)
normalizer2 = AdapterGenericNormalizer()   # Formato genérico (JSON string)
normalizer3 = MiNormalizerCustom()         # Tu normalizer personalizado
```

### **3. Reutilización**

Un **Parser** puede usarse para múltiples sitios, y cada sitio tiene su propio **Normalizer**:

```python
# Parser genérico HTML
parser = AdapterBs4Parser()

# Normalizers específicos por sitio
bcra_normalizer = AdapterBcraNormalizer()      # BCRA específico
scrapethissite_normalizer = AdapterScrapeThisSiteNormalizer()  # ScrapeThisSite específico
```

### **4. Transformaciones Específicas**

El **Normalizer** maneja transformaciones específicas del dominio:

#### **Ejemplo: AdapterBcraNormalizer**

**Transformaciones que hace:**

1. **Fechas**: 
   - Input: `"29/10/2025"` (formato BCRA)
   - Output: `"2025-10-29"` (ISO 8601)

2. **Valores numéricos**:
   - Input: `"40.764"` (string con separadores)
   - Output: `40764.0` (float)

3. **Estructura de contenido**:
   - Input: `Record.data = {detalle: "X", fecha: "29/10/2025", valor: "40.764"}`
   - Output: `Item.content = {fecha: "2025-10-29", valor: 40764.0}`

#### **Ejemplo: AdapterGenericNormalizer**

**Transformaciones que hace:**

- Serializa `Record.data` como JSON string
- Extrae título automáticamente si existe en los datos
- Fallback genérico cuando no hay normalizer específico

## 📊 Comparación: Con vs Sin Normalizer

### **Sin Normalizer (Arquitectura Antigua)**

```python
# Parser tenía que hacer TODO:
def parse(self, html, url):
    # Extraer datos
    detalle = extract_detalle(html)
    fecha = extract_fecha(html)
    valor = extract_valor(html)
    
    # Transformar datos
    fecha_iso = parse_fecha(fecha)  # ← Transformación mezclada
    valor_num = parse_valor(valor)  # ← Transformación mezclada
    
    # Crear Item directamente
    return Item(
        title=detalle,
        content=f"Fecha: {fecha} | Valor: {valor}",  # ← Formato fijo
        url=url
    )
```

**Problemas:**
- Parser tiene demasiadas responsabilidades
- Difícil cambiar el formato sin modificar el parser
- No se puede reutilizar el parser con diferentes formatos

### **Con Normalizer (Arquitectura Nueva)**

```python
# Parser solo extrae datos:
def parse(self, document):
    detalle = extract_detalle(document.text)
    fecha = extract_fecha(document.text)
    valor = extract_valor(document.text)
    
    # Retorna Record genérico (sin transformar)
    return Record(
        data={"detalle": detalle, "fecha": fecha, "valor": valor},
        source_url=document.url,
        fetched_at=datetime.now()
    )

# Normalizer transforma:
def normalize(self, records):
    for record in records:
        fecha_iso = parse_fecha(record.data["fecha"])  # ← Transformación separada
        valor_num = parse_valor(record.data["valor"])   # ← Transformación separada
        
        return Item(
            title=record.data["detalle"],
            content={"fecha": fecha_iso, "valor": valor_num},  # ← Formato estructurado
            url=record.source_url
        )
```

**Ventajas:**
- ✅ Separación clara de responsabilidades
- ✅ Parser reutilizable
- ✅ Normalizer intercambiable
- ✅ Fácil cambiar formato sin modificar parser

## 🎓 Casos de Uso

### **Caso 1: Mismo Parser, Diferentes Formatos**

```python
# Parser BCRA extrae los mismos datos
parser = AdapterBcraParser()

# Normalizer 1: Formato estructurado (JSON)
normalizer1 = AdapterBcraNormalizer()
# Output: {"fecha": "2025-10-29", "valor": 40764.0}

# Normalizer 2: Formato texto (legacy)
normalizer2 = AdapterBcraTextNormalizer()  # Si existiera
# Output: "Fecha: 29/10/2025 | Valor: 40.764"
```

### **Caso 2: Diferentes Sitios, Mismo Parser Base**

```python
# Parser genérico HTML
parser = AdapterBs4Parser()

# Cada sitio tiene su normalizer
bcra_normalizer = AdapterBcraNormalizer()
scrapethissite_normalizer = AdapterScrapeThisSiteNormalizer()
```

### **Caso 3: Validación y Limpieza**

El Normalizer puede validar y limpiar datos antes de crear Items:

```python
def normalize(self, records):
    for record in records:
        # Validar que fecha existe
        if not record.data.get("fecha"):
            continue  # Skip invalid records
        
        # Limpiar valor (remover espacios, validar formato)
        valor_limpio = self._limpiar_valor(record.data["valor"])
        
        # Transformar a formato estándar
        fecha_iso = self._parse_fecha(record.data["fecha"])
        valor_num = self._parse_valor(valor_limpio)
        
        return Item(...)
```

## 🔍 Resumen

| Componente | Responsabilidad | Entrada | Salida |
|------------|----------------|---------|--------|
| **Parser** | Extraer datos del documento | `Document` | `List[Record]` |
| **Normalizer** | Transformar a formato del dominio | `List[Record]` | `List[Item]` |
| **Output** | Emitir resultados | `List[Item]` | JSON/Archivo/Consola |

## 💡 Analogía Simple

Imagina que estás cocinando:

- **Parser** = Extraer ingredientes de la tienda (tomates, cebolla, carne)
- **Normalizer** = Preparar los ingredientes (cortar, pelar, marinar)
- **Output** = Servir el plato final (formato presentación)

El Normalizer es la capa que **prepara y transforma** los datos crudos del Parser en el formato estructurado que necesitas (con fechas ISO, números correctos, estructura específica).

## ✅ Ventajas del Normalizer

1. **Separación de responsabilidades**: Parser extrae, Normalizer transforma
2. **Reutilización**: Un parser puede usarse con múltiples normalizers
3. **Flexibilidad**: Fácil cambiar formato sin modificar parser
4. **Mantenibilidad**: Cambios en transformación no afectan extracción
5. **Testabilidad**: Puedes testear parser y normalizer por separado

