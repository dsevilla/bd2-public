import csv
import sys
from datetime import datetime
from typing import Any
from collections.abc import Generator, Callable
from pymongo.collection import Collection
from tqdm.notebook import tqdm

def csv_to_mongo(file: str, coll: Collection) -> None:
    """
    Carga un fichero CSV en Mongo. file especifica el fichero y coll la colección
    dentro de la base de datos.
    """
    # Convertir todos los elementos que se puedan a números
    def to_numeric(d: str) -> str | int | float:
        if len(d) == 0:
            return ''
        if not ((d[0] >= '0' and d[0] <= '9') or d[0] == '-' or d[0] == '+' or d[0]=='.'):
            return d
        try:
            v = int(d)
            return v if abs(v) <= sys.maxsize else d # avoid mongo errors with big integers
        except ValueError:
            try:
                return float(d)
            except ValueError:
                return d

    def to_date(d: str) -> datetime | None:
        """To ISO Date. If this cannot be converted, return NULL (None)"""
        try:
            return datetime.strptime(d, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            return None

    def batched(iterable, n) -> Generator[tuple, Any, None]:
        from itertools import islice
        if n < 1:
            raise ValueError('n must be at least one')
        it = iter(iterable)
        while batch := tuple(islice(it, n)):
            yield batch

    # Eliminar la colección
    coll.drop()

    with open(file, encoding='utf-8') as f:
        # La llamada csv.reader() crea un iterador sobre un fichero CSV
        reader = csv.reader(f, dialect='excel')

        # Se leen las columnas. Sus nombres se usarán para crear las diferentes columnas en la familia
        columns: list[str] = next(reader)

        # Las columnas que contienen 'Date' se interpretan como fechas
        func_to_cols: list[Callable] = list(map(lambda c: to_date if 'date' in c.lower() else to_numeric, columns))

        for batch in batched(tqdm(reader, desc='Leyendo e insertando filas...'), 10000):
            docs: list[dict] = []
            for row in batch:
                row = map(lambda fe : fe[0](fe[1]), zip(func_to_cols, row))
                docs.append(dict(zip(columns,row)))

            coll.insert_many(docs)
