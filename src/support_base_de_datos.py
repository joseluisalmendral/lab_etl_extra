import os
import dotenv
import psycopg2
import pandas as pd



def obtener_conexion_bd():
    """
    Establece y devuelve una conexión y cursor a la base de datos PostgreSQL.

    La función carga las variables de entorno desde un archivo `.env` usando la librería `dotenv`
    para obtener los parámetros de conexión, tales como el host, nombre de la base de datos, usuario,
    contraseña y puerto. Luego, crea una conexión y un cursor a la base de datos usando la librería `psycopg2`.
    
    Returns:
        tuple: Una tupla que contiene dos elementos:
            - conex (psycopg2.extensions.connection): La conexión a la base de datos.
            - cur (psycopg2.extensions.cursor): El cursor de la conexión, utilizado para ejecutar consultas SQL.

    Raises:
        psycopg2.OperationalError: Si ocurre algún error al intentar conectar con la base de datos.

    Ejemplo:
        >>> conex, cur = obtener_conexion_bd()
        >>> cur.execute("SELECT * FROM tabla")
        >>> resultados = cur.fetchall()
        >>> conex.commit()
        >>> cur.close()
        >>> conex.close()

    Nota:
        Es importante cerrar tanto el cursor (`cur.close()`) como la conexión (`conex.close()`)
        una vez que se termine de usar para liberar recursos.
    """
    dotenv.load_dotenv()

    conex = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT')
    )
    cur = conex.cursor()
    return conex, cur


def guardar_cambios_cerrar_conexion(conexion, cursor):
    """
    Confirma los cambios en la base de datos y cierra la conexión.

    Esta función ejecuta `commit()` en la conexión proporcionada para guardar
    todos los cambios pendientes en la base de datos. Luego, cierra tanto el cursor
    como la conexión para liberar recursos.

    Args:
        conexion (psycopg2.extensions.connection): La conexión a la base de datos que se debe cerrar.
        cursor (psycopg2.extensions.cursor): El cursor asociado a la conexión que se debe cerrar.

    Returns:
        None

    Ejemplo:
        >>> conex, cur = obtener_conexion_bd()
        >>> cur.execute("INSERT INTO tabla (columna) VALUES (%s)", ('valor',))
        >>> guardar_cambios_cerrar_conexion(conex, cur)

    Nota:
        Esta función debe utilizarse siempre que se haya completado el trabajo con la base de datos
        para asegurarse de que los cambios se guarden correctamente y que los recursos se liberen.

    """
    conexion.commit()
    cursor.close()
    conexion.close()


def generate_insert_script(table_name, dataframe):
    columns = ", ".join(dataframe.columns)
    values = ",\n".join(
        f"({', '.join(['%s' if pd.isna(val) else repr(val) for val in row])})" for row in dataframe.values
    )
    return f"INSERT INTO {table_name} ({columns}) VALUES\n{values};"