"""
🏥 Proyecto SaludDigitalA_E1 
----------------------------------------------------
Este script centraliza la ejecución del proyecto Big Data del sector Salud,
automatizando cada fase desarrollada en los notebooks dentro de la carpeta 'scripts/'.

Fases del flujo:
1️⃣ Creación de carpetas y estructura base
2️⃣ Creación del archivo CSV base
3️⃣ Generación de datos aleatorios (pacientes.csv)
4️⃣ Limpieza y ETL de los datos
5️⃣ Carga de datos limpios a MongoDB
6️⃣ Visualización y reportes analíticos
"""
# ==========================================
# 📦 Librerías necesarias
# ==========================================

# Instalar en consola si aún no las tienes:
# pip install nbformat nbconvert

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import os

# ==========================================
# ⚙️ Función general para ejecutar notebooks
# ==========================================

def ejecutar_notebook(ruta_notebook):
    """
    Ejecuta un archivo .ipynb usando nbconvert.
    """
    if not os.path.exists(ruta_notebook):
        print(f"No se encontró el archivo: {ruta_notebook}")
        return

    print(f"\n Ejecutando: {ruta_notebook}")
    try:
        with open(ruta_notebook, "r", encoding="utf-8") as f:
            nb = nbformat.read(f, as_version=4)
            ep = ExecutePreprocessor(timeout=None, kernel_name="python3")
            ep.preprocess(nb, {"metadata": {"path": os.path.dirname(ruta_notebook)}})

        print(f"{os.path.basename(ruta_notebook)} ejecutado correctamente.\n")
    except Exception as e:
        print(f"Error al ejecutar {ruta_notebook}: {e}\n")

# ==========================================
# 🧩 Fases del proyecto
# ==========================================

def fase_1():
    ejecutar_notebook("scripts/1_Crear_Carpetas.ipynb")

def fase_2():
    ejecutar_notebook("scripts/2_Crear_Estructura.ipynb")

def fase_3():
    ejecutar_notebook("scripts/3_Generar_Data.ipynb")

def fase_4():
    ejecutar_notebook("scripts/4_Proceso_ETL.ipynb")

def fase_5():
    ejecutar_notebook("scripts/5_Loading_MongoDB.ipynb")

def fase_6():
    ejecutar_notebook("scripts/6_Reportes.ipynb")

# ==========================================
# 🔁 Ejecución completa del pipeline
# ==========================================

def ejecutar_todo():
    print("\nEjecutando el flujo completo del proyecto Salud Digital...\n")
    fase_1()
    fase_2()
    fase_3()
    fase_4()
    fase_5()
    fase_6()
    print("\nFlujo completo ejecutado con éxito.\n")

# ==========================================
# 🧭 Menú principal
# ==========================================

def main():
    while True:
        print("""
==============================================
🏥 PROYECTO BIG DATA – GESTIÓN DE DATOS SALUD
==============================================

Seleccione una fase a ejecutar:
1️⃣  Crear estructura de carpetas y base
2️⃣  Crear archivo CSV base
3️⃣  Generar datos aleatorios de pacientes
4️⃣  Procesar ETL y limpiar datos
5️⃣  Cargar datos en MongoDB
6️⃣  Visualizar reportes analíticos
7️⃣  Ejecutar TODO el flujo completo
0️⃣  Salir
""")
        opcion = input("Ingrese una opción (0-7): ")

        if opcion == "1":
            fase_1()
        elif opcion == "2":
            fase_2()
        elif opcion == "3":
            fase_3()
        elif opcion == "4":
            fase_4()
        elif opcion == "5":
            fase_5()
        elif opcion == "6":
            fase_6()
        elif opcion == "7":
            ejecutar_todo()
        elif opcion == "0":
            print("Saliendo del programa... ¡Hasta pronto!")
            break
        else:
            print("Opción no válida. Intente nuevamente.")

# ==========================================
# 🚀 Punto de entrada principal
# ==========================================

if __name__ == "__main__":
    main()
