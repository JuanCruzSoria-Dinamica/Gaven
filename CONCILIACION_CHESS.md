# De-para: reporte de Chess ↔ Tablero Comercial

Para cuando alguien manda el **REPORTE DE COMISIONES POR VENTA** de Chess y
dice que "hay una diferencia de kgs" contra el tablero.

**La diferencia es esperada.** El tablero no es el reporte de Chess: replica el
Excel comercial (criterios acordados con Tomás, jul-2026), que saca ventas a
propósito y además mide los kilos con otra columna. Lo que hay que poder decir
es *cuánto* de la diferencia explica cada cosa.

Para sacar esos números de un mes concreto:

```bash
python3 conciliacion_chess.py --mes 2026-08
```

Es de solo lectura (no toca el parquet ni la serie). Imprime el de-para de
columnas, la cascada filtro por filtro, las cuatro formas de contar kilos y la
tabla vendedor por vendedor.

---

## 1. No es "por las etiquetas"

La marca / línea (`marca_linea`) se asigna por lookup de código de artículo
(`data/proveedor_objetivo_lookup.csv`) y **los SKUs que no están en la tabla
caen al nombre del proveedor** (`agregar_marca_linea`, data_pipeline.py:198).
Nunca se descarta una fila por no tener etiqueta.

Conclusión: una etiqueta mal puesta **mueve kilos de una fila a otra** dentro
del tablero (una marca sube, otra baja), pero **no cambia el total**. Si la
diferencia está en el TOTAL, no son las etiquetas. Las dos causas reales están
abajo.

## 2. Causa A: el tablero mide kilos con otra columna

| Reporte de Chess | Campo del API | Tablero |
|---|---|---|
| BULTOS | `cantidadesTotal` | no es el KPI; el tablero usa `bultos_cargo` (solo lo cobrado) |
| UNIDAD DE MEDIDA | `unimedtotal` | **no es la columna de "Kilos vendidos"** |
| IMPORTE | `subtotalNeto` (neto) o `subtotalFinal` (con IVA) — verificar en el print | "Facturación" = `subtotalNeto`, siempre sin IVA |

El KPI **"Kilos vendidos"** del tablero es (`recalcular_costo`,
data_pipeline.py:425):

```
kilos = pesoTotal   si pesoTotal != 0     (peso real del artículo pesable)
        unimedtotal si pesoTotal == 0     (el artículo no registra peso)
```

O sea: en todo lo **pesable** el tablero muestra el **peso real**, no la unidad
de medida facturada. Comparar "UNIDAD DE MEDIDA" de Chess contra "Kilos
vendidos" del tablero es comparar dos columnas distintas, aunque las dos digan
"kg". El paso 3 del script muestra las dos sumas por separado.

## 3. Causa B: el tablero saca ventas a propósito

Filtros de `preparar()` (data_pipeline.py:381-388), en orden:

1. Comprobantes **anulados** (`anulado != NO`).
2. Canal **VIANDAS**.
3. Vendedor **DIRECTA**.
4. Subcanal **VIANDAS**.
5. **Clientes excluidos**: 194, 762, 1043, 1046, 1050, 1054.
6. **Artículos excluidos**: 0 (conceptos: NC/ND por diferencia de precios,
   acuerdos comerciales, rechazo de cheques) y 1000 (viandas de refrigerio).

Chess no saca nada de eso, así que **siempre va a dar más alto**.

### Lo que ya se puede leer del reporte de agosto 2026

Del reporte que circuló (01/08/2026 – 31/08/2026, Casa Central):

| | Bultos | Unidad de medida | Importe |
|---|---|---|---|
| Total Chess | 47.976,89 | 256.614,07 | 1.592.892.105,37 |
| (-) `14 - DIRECTA` | -10.810,46 | -10.932,47 | -182.132.198,79 |
| Comparable (antes del resto) | 37.166,43 | 245.681,60 | 1.410.759.906,58 |

**DIRECTA sola explica 10.932 kg (4,3% de la unidad de medida) y $182 M
(11,4% del importe).** Es el primer descuento y el más grande de los visibles;
viandas, anulados, clientes y artículos excluidos van encima de eso y no se
pueden leer desde el reporte (los da el script).

Ojo con dos filas que **el tablero SÍ cuenta**, por si del otro lado las
excluyen: `36 - RETAIL VENTAS` (4.385,25) y `75 - FOOD CABA` (1.387,32).

## 4. A verificar contra los datos (no se puede desde el código)

- **Prefijo de código en los campos de texto.** Los filtros de DIRECTA y
  VIANDAS comparan contra el nombre pelado (`== "DIRECTA"`), pero el reporte de
  Chess muestra los vendedores como `14 - DIRECTA`. Si el API devuelve
  `dsVendedor` con ese prefijo, **el filtro no está sacando nada** y el tablero
  está contando DIRECTA (el campo `proveedor` sí viene con prefijo: por eso
  existe `_prov_limpio`, data_pipeline.py:159). El script avisa al final si
  detecta prefijos. Si avisa, hay que normalizar el campo antes de comparar
  (o filtrar por `idVendedor`), y el total del tablero va a bajar.
- **Qué es "IMPORTE" en el reporte**: neto o con IVA. El paso 1 del script
  imprime las dos sumas crudas; la que coincida con el reporte es la buena.
- **Sucursal**: el reporte sale de una sucursal (Casa Central). El tablero no
  filtra por empresa/sucursal: toma todo lo que devuelve el API.
- **Corte de datos**: comparar contra el mismo momento. El pipeline re-trae el
  mes actual y el anterior en cada corrida, así que un cierre mirado antes de
  la corrida de la noche puede tener comprobantes de menos. En el tablero está
  arriba: "Última actualización".

## 5. Orden para responder un "hay diferencia de kgs"

1. Correr `python3 conciliacion_chess.py --mes YYYY-MM`.
2. Paso 1: confirmar que el CRUDO coincide con el reporte de Chess. Si ya ahí
   no coincide, el problema es de alcance (sucursal, fechas, corte), no del
   tablero.
3. Paso 2: la cascada dice cuánto se lleva cada filtro. Esa es la respuesta.
4. Paso 3: si sobra diferencia después de la cascada, es la columna de kilos
   (`pesoTotal` vs `unimedtotal`), no las filas.
5. Paso 4: sirve para contestar por vendedor, que es como lo van a preguntar.
