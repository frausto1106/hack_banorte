import signal
import sys
from types import FrameType
from google.cloud.sql.connector import Connector


from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import SQLAlchemyError

from flask import Flask, jsonify, Response, request, redirect


from utils.logging import logger
import logging
import requests


from sqlalchemy.sql import text

# Iniciar la aplicación Flask
app = Flask(__name__)

# Iniciar el conector de Cloud SQL
connector = Connector()

def getconn():
    # Parámetros de conexión
    user = "user"
    password = "hack19"
    db = "hack"

    # Conectar a la base de datos
    conn = connector.connect(
        "hackbanorte:us-central1:hacki",
        "pg8000",
        user=user,
        password=password,
        db=db,
        ip_type="public"
    )
    return conn

# Configuración de SQLAlchemy para utilizar el conector de Cloud SQL
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql+pg8000://"
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    "creator": getconn
}

# Inicializar SQLAlchemy
dbp = SQLAlchemy()
dbp.init_app(app)

@app.route("/create_user/<string:id_cliente>")
def create_user(id_cliente):


    try:
        # Preparar la consulta para insertar el usuario
        query = text("""
            INSERT INTO "usuario" (id_cliente)
    VALUES (:id_cliente)
    ON CONFLICT (id_cliente) DO NOTHING
        """)

        # Ejecutar la consulta
        dbp.session.execute(query, {"id_cliente": id_cliente})

        # Confirmar los cambios en la base de datos
        dbp.session.commit()

        # Retornar una respuesta exitosa
        return jsonify({"message": f"Usuario {id_cliente} creado con éxito"}), 200
    except SQLAlchemyError as e:
        # Manejo de errores de base de datos
        dbp.session.rollback()  # Revertir cambios si ocurre un error
        return jsonify({"error": str(e)}), 500

@app.route("/")
def hello() -> str:
    """
    Ruta de prueba para verificar el estado de la aplicación.
    """
    # Usar logging básico con campos personalizados
    logger.info(logField="custom-entry", arbitraryField="custom-entry")

    # Agregar un log con el ID de la traza
    logger.info("Child logger with trace Id.")

    return "Hello, World!"

def shutdown_handler(signal_int: int, frame: FrameType) -> None:
    """
    Manejar la señal de apagado para asegurarse de que los logs se limpien correctamente.
    """
    logger.info(f"Caught Signal {signal.strsignal(signal_int)}")

    from utils.logging import flush

    # Limpiar logs
    flush()

    # Cerrar el programa de forma segura
    sys.exit(0)

if __name__ == "__main__":
    # Ejecutar la aplicación localmente, fuera de Google Cloud

    # Manejar la terminación con Ctrl-C
    signal.signal(signal.SIGINT, shutdown_handler)

    # Iniciar la aplicación Flask en modo de depuración
    app.run(host="localhost", port=8080, debug=True)
else:
    # Manejar la terminación del contenedor en Cloud Run
    signal.signal(signal.SIGTERM, shutdown_handler)


