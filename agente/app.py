import os
import signal
import sys
from types import FrameType
import pandas as pd
import io
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import SQLAlchemyError

from functools import wraps


from flask import Flask, jsonify, Response, request, redirect
import json
from firebase_admin import credentials, storage, auth, initialize_app, app_check


from utils.logging import logger
from flask_sqlalchemy import SQLAlchemy
import logging
import requests
from google.cloud.sql.connector import Connector
import vertexai
import firebase_admin
from google.cloud import texttospeech
from vertexai.generative_models import (
    GenerativeModel,
    GenerationConfig,
    SafetySetting,
    HarmCategory,
    HarmBlockThreshold,
)
import re
from sqlalchemy.sql import text


app = Flask(__name__)
connector = Connector()


def getconn():
    user = "user"
    password = "hack19"
    db = "hack"

    conn = connector.connect(
        "hackbanorte:us-central1:hacki",
        "pg8000",
        user=user,
        password=password,
        db=db,
        ip_type="public"

    )
    return conn



app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql+pg8000://"
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    "creator": getconn
}

dbp= SQLAlchemy()
dbp.init_app(app)

vertexai.init(project="hackbanorte", location="us-central1")
model = GenerativeModel("gemini-1.5-pro-002")
def gen_text(prompt):
    response = model.generate_content(
        [prompt]
    )
    responseT = response.text
    return  responseT



def vectorize_data_to_csv(user_id):
    # Consultar las tablas Ingreso_Casual y Gastos_Casual como texto SQL
    ingreso_query = text(f"""
        SELECT concepto, monto, fecha, hora, cuenta_proviniente, cuenta_entrante, categoria, estatus
        FROM Ingreso_Casual
        WHERE ID_cliente = :user_id
    """)
    gasto_query = text(f"""
        SELECT concepto, monto, fecha, hora, cuenta_proviniente, cuenta_entrante, categoria, estatus
        FROM Gastos_Casual
        WHERE ID_cliente = :user_id
    """)

    try:
        # Ejecuta las consultas y convierte el resultado a DataFrames de pandas
        ingresos_result = dbp.session.execute(ingreso_query, {'user_id': user_id}).fetchall()
        gastos_result = dbp.session.execute(gasto_query, {'user_id': user_id}).fetchall()

        # Convertir los resultados a DataFrames
        ingresos_df = pd.DataFrame(ingresos_result, columns=['concepto', 'monto', 'fecha', 'hora', 'cuenta_proviniente', 'cuenta_entrante', 'categoria', 'estatus'])
        gastos_df = pd.DataFrame(gastos_result, columns=['concepto', 'monto', 'fecha', 'hora', 'cuenta_proviniente', 'cuenta_entrante', 'categoria', 'estatus'])

        # Escribe los DataFrames a archivos CSV en memoria
        ingresos_output = io.StringIO()
        gastos_output = io.StringIO()

        ingresos_df.to_csv(ingresos_output, index=False)
        gastos_df.to_csv(gastos_output, index=False)

        # Devuelve los datos CSV como cadenas
        return {
            'ingresos_csv': ingresos_output.getvalue(),
            'gastos_csv': gastos_output.getvalue()
        }
    except SQLAlchemyError as e:
        app.logger.error(f"Error al ejecutar la consulta: {e}")
        return {
            'ingresos_csv': '',
            'gastos_csv': ''
        }

@app.route('/agent_report/<string:user_id>', methods=['GET'])
def show_csv(user_id):
    # Generar los CSVs en memoria
    csv_data = vectorize_data_to_csv(user_id)

    # Combinar los contenidos de los archivos CSV (ingresos y gastos)
    combined_csv_data = (
        "Datos de Ingresos Casuales:\n\n" + csv_data['ingresos_csv'] + "\n\n" +
        "Datos de Gastos Casuales:\n\n" + csv_data['gastos_csv']
    )

    prompt = (f"toma en cuenta que el id del usuario es  {user_id} analiza el siguiente conjunto de datos el cual esta en un dataframe parecido a csv \n{csv_data['gastos_csv']} \nson los gastos de una persona\n"
              f"necesito que me digas el id de los gastos que se repiten con un frecuencia determinada, para considerarla recurrente tiene que repetirse por lo menos tres veces y al final haz un valance mensual del gasto respecto a estas 8 categorias"
              f"Restaurante, Transporte, Entretenimiento, Bienestar, Educación, Vestimenta, Tecnología, Servicios, la forma en la que deberas contestar esta dado en un sistema de etiquetas el cual yo te proporcionare,SIMILAR A HTML, NO LE AGREGUES NADA A TU RESPUESTA SOLO CONTESTA CON LA SINTAXIS DE LAS ETIQUETAS, LA RESPUESTA SE DEBE PRESENTAR DE LA SIGUIENTE MANERA, VALOR ENTRE ETIQUETAS, DE OTRA MANERA EL SISTEMA NO LO ENTIENDE , ES COMO SI FUERA UN SISTEMA DE MARCADO PARECIDO A HTML, ENTONCES OCUPA EL SIGUIENTE FORMATO PARA PRESENTAR LA INFORMACION <ETIQUETA>VALOR</ETIQUETA> \n"
              f"<restaurante>gasto restaurante</restaurante>"
              f"<transporte>gasto transporte</transporte>"
              f"<entretenimiento>gasto entretenimiento</entretenimiento>"
              f"<bienestar>gasto bienestar</bienestar>"
              f"<educacion>gasto educacion</educacion>"
              f"<Vestimenta>gasto Vestimenta</Vestimenta>"
              f"<Tecnología>gasto Tecnología</Tecnología>"
              f"<Servicios>gasto Servicios</Servicios>"
              f"<gastoR>escribe un query en que lo introduzas en la tabla: CREATE TABLE gastos_recurrentes (ID_cliente VARCHAR(100), ID_gasto UUID DEFAULT gen_random_uuid() PRIMARY KEY,frecuencia int  ,  concepto VARCHAR(100),monto MONEY NOT NULL,fecha DATE NOT NULL hora TIME NOT NULL,cuenta_proviniente INT,cuenta_entrante INT,categoria VARCHAR(100),estatus VARCHAR(100),id_comprobante_gr INT,FOREIGN KEY (ID_cliente) REFERENCES Usuario(ID_cliente) ON DELETE CASCADE); donde frecuencia esta dada en dias, osea cada cuantos dias se repite, y fecha es la fecha aproximada de cada mes que va a suceder</gastoR>"
              
              f"despues analiza el siguiente conjunto de datos el cual esta en un dataframe parecido a csv \n{csv_data['ingresos_csv']} \nson los ingresos de una persona\n"
              f"necesito que me digas el id de los ingresos que se repiten con un frecuencia determinada, para considerarla recurrente tiene que repetirse por lo menos tres veces y al final haz un valance mensual del ingresos totales, LA RESPUESTA SE DEBE PRESENTAR DE LA SIGUIENTE MANERA, VALOR ENTRE ETIQUETAS"
              f"<ingresoR>escribe un query en que lo introduzas en la tabla: CREATE TABLE ingreso_recurrentes (ID_cliente VARCHAR(100), ID_ingreso UUID DEFAULT gen_random_uuid() PRIMARY KEY,frecuencia int ,   concepto VARCHAR(100),monto MONEY NOT NULL,fecha DATE NOT NULL hora TIME NOT NULL,cuenta_proviniente INT,cuenta_entrante INT,categoria VARCHAR(100),estatus VARCHAR(100),id_comprobante_gr INT,frecuencia INT,FOREIGN KEY (ID_cliente) REFERENCES Usuario(ID_cliente) ON DELETE CASCADE); donde frecuencia esta dada en dias, osea cada cuantos dias se repite, y fecha es la fecha aproximada de cada mes que va a suceder</ingresoR>\n"
              f"para finalizar genera unos presupuesto por cada categoria mensuales a partir de los ingresos totales mesuales promedio y la tendencia de gasto para cada categoria, LA RESPUESTA SE DEBE PRESENTAR DE LA SIGUIENTE MANERA, VALOR ENTRE ETIQUETAS"
              f"<restauranteB>presupuesto mensual restaurante</restauranteB>"
              f"<transporteB>presupuesto mensual transporte</transporteB>"
              f"<entretenimientoB>presupuesto mensual entretenimiento</entretenimientoB>"
              f"<bienestarB>presupuesto mensual bienestar</bienestarB>"
              f"<educacionB>presupuesto mensual educacion</educacionB>"
              f"<VestimentaB>presupuesto mensual Vestimenta</VestimentaB>"
              f"<TecnologíaB>presupuesto mensual Tecnología</TecnologíaB>"
              f"<ServiciosB>presupuesto mensual Servicios</ServiciosB>"
              f"RECUERDA SEGUIR LA SINTAXIS YA QUE MI SISTEMA NO ENTIENDO TUS RESPUESTAS DE OTRA MANERA"
              )
    response = gen_text(prompt)
    print(response)

    gasto_pattern = (
        r'<restaurante>(.*?)<\/restaurante>\s*'
        r'<transporte>(.*?)<\/transporte>\s*'
        r'<entretenimiento>(.*?)<\/entretenimiento>\s*'
        r'<bienestar>(.*?)<\/bienestar>\s*'
        r'<educacion>(.*?)<\/educacion>\s*'
        r'<Vestimenta>(.*?)<\/Vestimenta>\s*'
        r'<Tecnología>(.*?)<\/Tecnología>\s*'
        r'<Servicios>(.*?)<\/Servicios>'
    )

    presupuesto_pattern = (
        r'<restauranteB>(.*?)<\/restauranteB>\s*'
        r'<transporteB>(.*?)<\/transporteB>\s*'
        r'<entretenimientoB>(.*?)<\/entretenimientoB>\s*'
        r'<bienestarB>(.*?)<\/bienestarB>\s*'
        r'<educacionB>(.*?)<\/educacionB>\s*'
        r'<VestimentaB>(.*?)<\/VestimentaB>\s*'
        r'<TecnologíaB>(.*?)<\/TecnologíaB>\s*'
        r'<ServiciosB>(.*?)<\/ServiciosB>'
    )

    # Compilar expresiones regulares
    gasto_exp = re.compile(gasto_pattern, re.DOTALL)
    presupuesto_exp = re.compile(presupuesto_pattern, re.DOTALL)

    # Inicializar variables para gastos y presupuestos
    gasto_restaurante = gasto_transporte = gasto_entretenimiento = gasto_bienestar = gasto_educacion = gasto_vestimenta = gasto_tecnologia = gasto_servicios = None
    presupuesto_restaurante = presupuesto_transporte = presupuesto_entretenimiento = presupuesto_bienestar = presupuesto_educacion = presupuesto_vestimenta = presupuesto_tecnologia = presupuesto_servicios = None

    # Encontrar coincidencias para gastos
    gasto_matches = gasto_exp.findall(response)

    # Almacenar los gastos en variables
    if gasto_matches:
        for match in gasto_matches:
            gasto_restaurante = match[0].strip()
            gasto_transporte = match[1].strip()
            gasto_entretenimiento = match[2].strip()
            gasto_bienestar = match[3].strip()
            gasto_educacion = match[4].strip()
            gasto_vestimenta = match[5].strip()
            gasto_tecnologia = match[6].strip()
            gasto_servicios = match[7].strip()

    # Encontrar coincidencias para presupuestos
    presupuesto_matches = presupuesto_exp.findall(response)

    # Almacenar los presupuestos en variables
    if presupuesto_matches:
        for p_match in presupuesto_matches:
            presupuesto_restaurante = p_match[0].strip()
            presupuesto_transporte = p_match[1].strip()
            presupuesto_entretenimiento = p_match[2].strip()
            presupuesto_bienestar = p_match[3].strip()
            presupuesto_educacion = p_match[4].strip()
            presupuesto_vestimenta = p_match[5].strip()
            presupuesto_tecnologia = p_match[6].strip()
            presupuesto_servicios = p_match[7].strip()

    # Imprimir los resultados (opcional)
    print("Gastos:")
    print(f"Gasto Restaurante: {gasto_restaurante}")
    print(f"Gasto Transporte: {gasto_transporte}")
    print(f"Gasto Entretenimiento: {gasto_entretenimiento}")
    print(f"Gasto Bienestar: {gasto_bienestar}")
    print(f"Gasto Educación: {gasto_educacion}")
    print(f"Gasto Vestimenta: {gasto_vestimenta}")
    print(f"Gasto Tecnología: {gasto_tecnologia}")
    print(f"Gasto Servicios: {gasto_servicios}")

    print("\nPresupuestos:")
    print(f"Presupuesto Mensual Restaurante: {presupuesto_restaurante}")
    print(f"Presupuesto Mensual Transporte: {presupuesto_transporte}")
    print(f"Presupuesto Mensual Entretenimiento: {presupuesto_entretenimiento}")
    print(f"Presupuesto Mensual Bienestar: {presupuesto_bienestar}")
    print(f"Presupuesto Mensual Educación: {presupuesto_educacion}")
    print(f"Presupuesto Mensual Vestimenta: {presupuesto_vestimenta}")
    print(f"Presupuesto Mensual Tecnología: {presupuesto_tecnologia}")
    print(f"Presupuesto Mensual Servicios: {presupuesto_servicios}")

    gasto_pattern = r'<gastoR>(.*?)<\/gastoR>'
    ingreso_pattern = r'<ingresoR>(.*?)<\/ingresoR>'

    # Buscar coincidencias
    gasto_matches = re.findall(gasto_pattern, response, re.DOTALL)
    ingreso_matches = re.findall(ingreso_pattern, response, re.DOTALL)

    # Extraer los queries
    gastos_queries = [gasto.strip() for gasto in gasto_matches]
    ingresos_queries = [ingreso.strip() for ingreso in ingreso_matches]

    # Imprimir los resultados
    print("Queries de gastos:")
    for query in gastos_queries:
        print(query)
        dbp.session.execute(text(query))
        dbp.session.commit()

    print("\nQueries de ingresos:")
    for query in ingresos_queries:
        print(query)
        dbp.session.execute(text(query))
        dbp.session.commit()

    return response




@app.route("/")
def hello() -> str:
    # Use basic logging with custom fields
    logger.info(logField="custom-entry", arbitraryField="custom-entry")

    # https://cloud.google.com/run/docs/logging#correlate-logs
    logger.info("Child logger with trace Id.")

    return "Hello, World!"


def shutdown_handler(signal_int: int, frame: FrameType) -> None:
    logger.info(f"Caught Signal {signal.strsignal(signal_int)}")

    from utils.logging import flush

    flush()

    # Safely exit program
    sys.exit(0)


if __name__ == "__main__":
    # Running application locally, outside of a Google Cloud Environment

    # handles Ctrl-C termination
    signal.signal(signal.SIGINT, shutdown_handler)

    app.run(host="localhost", port=8080, debug=True)
else:
    # handles Cloud Run container termination
    signal.signal(signal.SIGTERM, shutdown_handler)
