import os
import time
import random
from faker import Faker
from sqlalchemy import create_engine, text
import oracledb
import pandas as pd

fake = Faker('es_ES')
PASS_SIMPLE = os.getenv('DB_PASS_SIMPLE', 'Udla2026_SeeleStore')
PASS_COMPLEX = os.getenv('DB_PASS_COMPLEX', 'Udla_2026_Secure!')

def get_mariadb():
    return create_engine(f'mysql+pymysql://root:{PASS_SIMPLE}@mariadb:3306/ComercioDB')

def get_oracle_conn():
    params = oracledb.ConnectParams(host="oracle", port=1521, service_name="XE")
    return oracledb.connect(user="system", password=PASS_SIMPLE, params=params)

def get_sqlserver_analytics():
    return create_engine(f'mssql+pyodbc://sa:{PASS_COMPLEX}@mssql:1433/AnalyticsDB?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes')


def menu():
    print("\n" + "="*50)
    print("CONSOLA DE MANDO SEELESTORE")
    print("="*50)
    print("1. [MANUAL] Insertar Nuevo Cliente (MariaDB)")
    print("2. [MANUAL] Insertar Venta Específica (Oracle)")
    print("3. [VISUAL] Ver Reporte Financiero (SQL Server)")
    print("4. [VISUAL] Ver Últimas Ventas CON NOMBRES (Heterogéneo)")
    print("5. [AUTO]   Generar Ráfaga Aleatoria (5 Ventas)")
    print("6. Salir")
    return input(">> Seleccione una opción: ")

def crear_cliente():
    print("\n--- NUEVO CLIENTE ---")
    nombre = input("Nombre del Cliente: ")
    ciudad = input("Ciudad: ")
    
    try:
        eng = get_mariadb()
        with eng.connect() as conn:
            conn.execute(text(f"INSERT INTO clientes (nombre, ciudad) VALUES ('{nombre}', '{ciudad}')"))
            result = conn.execute(text("SELECT MAX(id) FROM clientes")).scalar()
            print(f"✅ Cliente '{nombre}' guardado en MariaDB con ID: [ {result} ]")
    except Exception as e:
        print(f"❌ Error: {e}")

def crear_orden():
    print("\n--- 💰 NUEVA ORDEN DE VENTA ---")
    try:
        id_cliente = input("Ingrese ID del Cliente (Debe existir en MariaDB): ")
        total = input("Monto total de la venta (USD): ")
        
        conn = get_oracle_conn()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO ordenes (cliente_id, total, estado) VALUES (:1, :2, 'PAGADO')", 
                        (id_cliente, total))
        conn.commit()
        conn.close()
        print(f"✅ Orden de ${total} asignada al Cliente ID {id_cliente} en Oracle.")
    except Exception as e:
        print(f"❌ Error (¿Quizá el ID no es número?): {e}")

def ver_ventas_con_nombres():
    print("\n--- 🌐 VISUALIZADOR HETEROGÉNEO (Oracle + MariaDB) ---")
    print("Recuperando los últimos 5 tickets de Oracle y buscando sus dueños en MariaDB...")
    print("-" * 75)
    print(f"{'ORDEN':<8} | {'CLIENTE (MariaDB)':<30} | {'TOTAL (Oracle)':<15}")
    print("-" * 75)
    
    try:
        conn_ora = get_oracle_conn()
        cursor_ora = conn_ora.cursor()
        cursor_ora.execute("SELECT id, cliente_id, total FROM ordenes ORDER BY id DESC FETCH FIRST 5 ROWS ONLY")
        ordenes = cursor_ora.fetchall()
        conn_ora.close()

        eng_maria = get_mariadb()
        with eng_maria.connect() as conn_maria:
            for orden in ordenes:
                ora_id = orden[0]
                cli_id = orden[1]
                total = orden[2]
                res = conn_maria.execute(text(f"SELECT nombre FROM clientes WHERE id = {cli_id}"))
                nombre_cliente = res.scalar()
                
                if not nombre_cliente:
                    nombre_cliente = "--- Desconocido ---"
                
                print(f"{ora_id:<8} | {nombre_cliente:<30} | ${total:<15}")

    except Exception as e:
        print(f"❌ Error en la interconexión: {e}")

def ver_reporte():
    print("\n---REPORTE ANALÍTICO (SQL SERVER) ---")
    try:
        eng = get_sqlserver_analytics()
        with eng.connect() as conn:
            result = conn.execute(text("SELECT TOP 50 * FROM reporte_fin ORDER BY id DESC"))
            print(f"{'ID':<5} | {'FECHA':<22} | {'TOTAL($)':<10} | {'ORDENES':<5}")
            print("-" * 55)
            for row in result:
                fecha = str(row[1])[:19] # Cortar milisegundos
                print(f"{row[0]:<5} | {fecha:<22} | {row[2]:<10} | {row[3]:<5}")
    except Exception as e:
        print(f"❌ Error: {e}")

def generar_rafaga():
    print("\n---GENERANDO RÁFAGA DE DATOS (Faker) ---")
    try:
        eng = get_mariadb()
        nombres = []
        for _ in range(5):
            nom = fake.name()
            nombres.append({"nombre": nom, "ciudad": fake.city()})
        pd.DataFrame(nombres).to_sql('clientes', eng, if_exists='append', index=False)
        print(f"Se insertaron 5 clientes nuevos en MariaDB.")

        conn = get_oracle_conn()
        cursor = conn.cursor()
        for _ in range(5):
            uid = random.randint(1, 50) 
            monto = round(random.uniform(20, 500), 2)
            cursor.execute("INSERT INTO ordenes (cliente_id, total, estado) VALUES (:1, :2, 'PAGADO')", 
                        (uid, monto))
        conn.commit()
        conn.close()
        print(f"Se generaron 5 órdenes de compra en Oracle.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    while True:
        try:
            opc = menu()
            if opc == '1': crear_cliente()
            elif opc == '2': crear_orden()
            elif opc == '3': ver_reporte()
            elif opc == '4': ver_ventas_con_nombres()
            elif opc == '5': generar_rafaga()
            elif opc == '6': 
                print("Cerrando conexión digamos que es satelital...")
                break
            else:
                print("Opción no válida.")
        except KeyboardInterrupt:
            print("\nSalida forzada.")
            break