"""Genera data/familias_lookup.csv (idArticulo -> familia / tipo de producto).

Fuente: data/familias_tipo_producto.xlsx (lista comercial, columnas Proveedor,
Marca, Grupo, Familia, Tipo de producto, Producto). La lista viene por NOMBRE
de producto; acá se traduce a CÓDIGO de artículo cruzando contra todos los
nombres que ese código tuvo en ventas_actualizadas.parquet (Chess a veces
renombra artículos). El pipeline después joina por idArticulo.

Los SKUs con venta que NO están en la lista reciben una familia SUGERIDA por
parecido de nombre con un producto ya clasificado (origen = "sugerido"). Los
que no se parecen a nada quedan fuera del lookup y el panel los muestra como
"SIN ASIGNAR". Para revisar las sugerencias se genera
familias_revisar.xlsx: lo que se confirme se agrega a la lista comercial y se
vuelve a correr este script.

Uso:
    python build_lookup_familias.py
"""
import os
import re
import unicodedata

import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
XLSX = os.path.join(BASE, "data", "familias_tipo_producto.xlsx")
VENTAS = os.path.join(BASE, "data", "ventas_actualizadas.parquet")
SALIDA = os.path.join(BASE, "data", "familias_lookup.csv")
REVISAR = os.path.join(BASE, "familias_revisar.xlsx")

UMBRAL_SUGERENCIA = 0.34   # similitud mínima (Jaccard de palabras) para sugerir
RANGO_HELADOS = (3000, 3099)  # códigos de sabores de helado (sin tamaño en el nombre)

# Palabras que no ayudan a reconocer el producto (tamaños, unidades, conectores)
_RUIDO = {"X", "KG", "KGS", "GR", "GRS", "U", "UN", "UNI", "LT", "ML", "CC", "DE",
          "C", "Y", "CON", "EN", "P", "A", "V", "BULTO", "CM", "S", "LA", "EL"}


def norm(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s.upper()).strip()


# Marcas / fabricantes: se excluyen de la comparación para que dos productos
# no se parezcan solo por ser de la misma marca (ej. "DINOS DE POLLO GRANGYS"
# no es un medallón por compartir "GRANGYS"). Se completa con las columnas
# Proveedor y Marca de la lista.
_MARCAS = {"GTA", "GRANGYS", "TREGAR", "TONADITA", "PALADINI", "CAGNOLI", "FELA",
           "FRIAR", "VACALIN", "MILKAUT", "PAULINA", "NONNA", "PIA", "VAQUERO",
           "CAMPOS", "CACIQUE", "DOBLE", "SADIA", "VEGETALEX", "KAMAR", "MC",
           "CAIN", "MCCAIN", "BIOMAC", "EMPERADOR", "ABRATTO", "COMARCA", "JET",
           "FOOD", "CALVOS", "LOS", "CARCARANA", "TIVOLI", "RAG", "FABBRI"}


def palabras(s):
    toks = re.split(r"[^A-Z0-9]+", norm(s))
    return {t for t in toks if t and len(t) > 1 and t not in _RUIDO
            and t not in _MARCAS and not re.search(r"\d", t)}


# Reglas por palabra clave (se evalúan ANTES del parecido de nombre, en orden).
# Cubren productos sin un "hermano" en la lista pero cuya familia es obvia por
# el nombre. Igual quedan marcados como "sugerido" para revisar.
REGLAS = [
    (r"^YOGU?R", "Lacteos", "Yogurt"),
    (r"^DDL ", "Lacteos", "DDL"),
    (r"^MANT(\.|ECA) ", "Lacteos", "Manteca"),
    (r"^Q\. CREMA ", "Quesos", "Untables"),
    (r"PROVOL", "Quesos", "Duros"),
    (r"^Q\. SARDO", "Quesos", "Duros"),
    (r"(BOCADITO|NUGGETS|DINOS).*POLLO|NUGGETS|DINOS DE POLLO", "Prefritos", "Pollo"),
    (r"^BOCADITO ", "Prefritos", "Verduras"),
    (r"^(?!Q\.).*(MUZZARELLA|MOZZ)", "Prefritos", "Mozzarella"),
    (r"^MC CAIN (TRAD|FRITAS|GOLAZO|CROSSTRAX|BONIATO|CUNA)", "Papas", "Papas"),
]


def leer_lista():
    x = pd.read_excel(XLSX, header=1)
    x = x.loc[:, ~x.columns.astype(str).str.startswith("Unnamed")]
    x.columns = ["proveedor", "marca", "grupo", "familia", "tipo", "producto",
                 "familia_tipo"][:len(x.columns)]
    x = x.dropna(subset=["producto", "familia"])
    for c in ["familia", "tipo", "grupo", "producto"]:
        x[c] = x[c].astype(str).str.strip().replace({"nan": ""})
    x["k"] = x["producto"].map(norm)
    return x.drop_duplicates("k")


def main():
    lista = leer_lista()
    for c in ["proveedor", "marca"]:
        if c in lista.columns:
            for v in lista[c].dropna().astype(str):
                _MARCAS.update(palabras(v) - {"POLLO", "CERDO", "QUESO"})
    v = pd.read_parquet(VENTAS, columns=["idArticulo", "dsArticulo",
                                         "subtotalNeto", "fechaComprobate"])
    v["idArticulo"] = pd.to_numeric(v["idArticulo"], errors="coerce")
    v = v.dropna(subset=["idArticulo"])
    # Todos los nombres históricos de cada código
    nombres = v.drop_duplicates(["idArticulo", "dsArticulo"])[["idArticulo", "dsArticulo"]]
    nombres["k"] = nombres["dsArticulo"].map(norm)
    vigente = (v.sort_values("fechaComprobate").groupby("idArticulo")
               .agg(producto=("dsArticulo", "last"), neto=("subtotalNeto", "sum"))
               .reset_index())

    # 1) Cruce exacto por nombre normalizado
    m = nombres.merge(lista[["k", "familia", "tipo", "grupo", "producto"]]
                      .rename(columns={"producto": "producto_lista"}), on="k")
    m = m.drop_duplicates("idArticulo")
    m["origen"] = "lista"
    m["similar_a"] = ""
    m["similitud"] = 1.0
    asignados = m[["idArticulo", "familia", "tipo", "grupo", "origen",
                   "similar_a", "similitud"]]

    # 2) Sugerencias para los códigos con venta que no están en la lista
    ref = lista.assign(pal=lista["producto"].map(palabras))
    faltan = vigente[~vigente["idArticulo"].isin(asignados["idArticulo"])]
    sug = []
    for _, r in faltan.iterrows():
        cod = int(r["idArticulo"])
        if RANGO_HELADOS[0] <= cod <= RANGO_HELADOS[1]:
            sug.append((cod, "Helados", "Helados", "", "sugerido",
                        "código en el rango de sabores de helado", 1.0))
            continue
        nom = norm(r["producto"])
        regla = next((x for x in REGLAS if re.search(x[0], nom)), None)
        if regla:
            sug.append((cod, regla[1], regla[2], "", "sugerido",
                        f"regla: {regla[0]}", 1.0))
            continue
        p = palabras(r["producto"])
        if not p:
            continue
        sims = ref["pal"].map(lambda q: len(p & q) / len(p | q) if q else 0)
        i = sims.idxmax()
        if sims[i] >= UMBRAL_SUGERENCIA:
            b = ref.loc[i]
            sug.append((cod, b["familia"], b["tipo"], b["grupo"], "sugerido",
                        b["producto"], round(float(sims[i]), 2)))
    sug = pd.DataFrame(sug, columns=asignados.columns)

    lookup = pd.concat([asignados, sug], ignore_index=True)
    lookup["idArticulo"] = lookup["idArticulo"].astype(int)
    lookup = lookup.merge(vigente[["idArticulo", "producto"]], on="idArticulo", how="left")
    lookup = lookup.sort_values(["familia", "tipo", "producto"])
    lookup[["idArticulo", "producto", "familia", "tipo", "grupo", "origen"]].to_csv(
        SALIDA, index=False, encoding="utf-8")

    # Excel de revisión: sugeridos + sin asignar, ordenados por facturación
    rev = vigente.merge(lookup[["idArticulo", "familia", "tipo", "origen",
                                "similar_a", "similitud"]],
                        on="idArticulo", how="left")
    rev = rev[rev["origen"] != "lista"].copy()
    rev["origen"] = rev["origen"].fillna("SIN ASIGNAR")
    rev = rev.sort_values(["origen", "neto"], ascending=[False, False])
    rev = rev.rename(columns={"idArticulo": "Código", "producto": "Producto",
                              "neto": "Venta neta 2026", "familia": "Familia sugerida",
                              "tipo": "Tipo sugerido", "origen": "Estado",
                              "similar_a": "Parecido a", "similitud": "Similitud"})
    rev.to_excel(REVISAR, index=False)

    tot = vigente["neto"].sum()
    def share(cods):
        return vigente.loc[vigente["idArticulo"].isin(cods), "neto"].sum() / tot
    print(f"Lista: {len(asignados)} SKUs ({share(asignados.idArticulo):.1%} de la venta)")
    print(f"Sugeridos: {len(sug)} SKUs ({share(sug.idArticulo):.1%})")
    sin = set(vigente.idArticulo) - set(lookup.idArticulo)
    print(f"Sin asignar: {len(sin)} SKUs ({share(sin):.1%})")
    print(f"-> {SALIDA}\n-> {REVISAR}")


if __name__ == "__main__":
    main()
