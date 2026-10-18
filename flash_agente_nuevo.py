# -*- coding: utf-8 -*-
"""
FLASH DE ACARREO - Agente V4
Minera Rio Tinto - Bascula Cieneguita, Urique, Chihuahua

Lee los WebServices JSON de RevueltaSIP (puerto 8060, solo lectura) y empuja
los boletos a Firebase. Disenado bajo una regla dura:

    NO DEBE INTERFERIR NI AFECTAR LA OPERACION DIARIA DE BASCULA.

Por eso:
  - Solo usa la libreria estandar de Python. Nada que instalar con pip.
  - Solo lee. Nunca escribe en la base de datos ni abre RevueltaSIP.
  - Timeouts cortos y duros. Si algo tarda, se rinde y lo intenta despues.
  - Si existe el archivo STOP en esta carpeta, no hace absolutamente nada.
  - No abre ventanas ni roba el foco del teclado.
  - Bitacora con tope de tamano, para no llenar el disco.
  - Si no hay internet, encola en disco y reintenta. Nunca pierde un boleto.
  - Nunca revienta: cualquier error se registra y el proceso termina limpio.

Idempotencia: cada boleto se escribe en Firebase con su NUMERO DE BOLETO como
llave (PUT, no POST). Correr esto cien veces sobre el mismo rango no duplica
un solo registro.

V4.0 - Campanas (lotes)
  Lee /campanas de Firebase y le pone a cada boleto el numero de lote al que
  pertenece, y al resumen del dia un desglose por lote. Es LECTURA de Firebase
  y escritura de un campo mas: Revuelta no se toca, el webservice se consulta
  igual que siempre y si /campanas no existe o viene mal, el agente trabaja
  exactamente como la V3.9. La regla de no interferir con bascula se mantiene.

V4.1 - Campanas por boleto y cierre automatico por tope
  Una campana ahora puede arrancar en un NUMERO DE BOLETO (desde_boleto),
  ademas del momento de la V4.0. Al superar su tope (tope_kg), el agente
  abre sola la siguiente: arranca en el boleto siguiente de esa procedencia
  (el camion que cruza el tope entra completo). Si el gestor dejo indicado
  el boleto de la siguiente (siguiente_boleto), se respeta ese. Cada apertura
  automatica queda en el historial de la campana. Todo va envuelto: si algo
  falla, los boletos se envian igual que en la V4.0.

V4.3 - Viajes de 2025 capturados a mano en Excel
  Mientras Revuelta estuvo caido (antes del 11-nov-25) los viajes se
  anotaron en Excel. La orden puede traer "excel": {id, url, sha256}; el
  agente baja ese archivo UNA vez, lo guarda junto a si mismo
  (excel_2025.json) y lo mezcla con lo de bascula cada vez que procesa uno
  de esos dias. Cada viaje lleva llave propia XLSnn-fila (nunca choca con un
  boleto real) y origen_dato = "excel". Bascula manda: el archivo solo trae
  dias sin bascula o viajes que bascula no tiene. Si el archivo falta o no
  cuadra su huella, el agente trabaja exactamente como la V4.2.

V4.4 - Alertas: bascula contra los Excel de fletes
  Lee en SOLO LECTURA los Excel de fletes de la carpeta compartida
  21.-Bascula\2026 (junto a Flash_Acarreo_Data) y los compara contra los
  boletos ya enviados a Firebase. Sube solo el resultado a /alertas. Corre
  cuando cambia algun Excel o cada 15 min. Nunca escribe en un Excel. Si
  algo falla, el agente trabaja exactamente como la V4.3.

V4.5 - Alerta de posible duplicado en bascula
  Mismo dia, mismo chofer o placas y casi el mismo bruto: se avisa en
  cuanto se pesa, sin esperar a que el viaje se capture en el Excel.

V4.6 - Duplicado: el chofer tiene que ser el mismo (tolera dedazos) y
  las placas no pueden ser distintas. "SN" ya no junta camiones.

V4.7 - Tarja, pesada manual y boletos excluidos
  - Folio de la tarja: llega de Revuelta en el "Dato adicional" (Adicion).
    Si no viene, se toma el FOLIO TARJA del Excel (/tarjas_excel).
  - Pesada manual: Bascula_PP / Bascula_SP = "M" -> el boleto lleva "pesada".
  - Los Excel ya traen el boleto (FOLIO REV): se cuadra directo por numero.
  - Alertas nuevas: tarja usada dos veces, folio de tarja distinto y
    boleto distinto en el Excel.
  - Boletos excluidos del Flash (/excluidos, los pone el visor con permiso
    especial y comentario). Siguen en Revuelta y en /boletos marcados, pero
    no cuentan en el resumen, en las campanas ni en el cuadre. Cuando
    alguien excluye o restaura, el agente recalcula solo ese dia.

V4.8 - Duplicado con su fila del Excel y sugerencia
  La alerta de posible duplicado trae la fila del Excel de cada boleto
  (archivo, fila, tarja) y dice cual conviene excluir y por que.
"""

import hashlib
import json
import os
import re
import shutil
import sys
import time
import unicodedata
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timedelta

VERSION = "4.8"
BASE = os.path.dirname(os.path.abspath(__file__))
ARCHIVO_CONFIG = os.path.join(BASE, "config.json")
ARCHIVO_STOP = os.path.join(BASE, "STOP")
ARCHIVO_LOG = os.path.join(BASE, "flash.log")
ARCHIVO_COLA = os.path.join(BASE, "cola_offline.jsonl")

LOG_MAX_BYTES = 2 * 1024 * 1024        # 2 MB y rota
COLA_MAX_LINEAS = 20000                # tope de la cola offline
LOTE_MAX        = 250                  # rutas por peticion a Firebase

CONFIG_DEFAULT = {
    "firebase_url": "https://flash-revuelta-mrt-csalido-default-rtdb.firebaseio.com",
    "firebase_auth": "",
    "ws_base": "http://localhost:8060/Revuelta",
    "ws_cerrados": "flash_acarreo",
    "ws_patio": "Flash_Patio",
    "dias_hacia_atras": 1,
    "timeout_ws": 12,
    "timeout_firebase": 20,
    "modo_observacion": True,
    "verbose": False,
    "carpeta_dropbox": "",
    "actualizacion_automatica": "dropbox",
    "orden_url": "",
    "timeout_orden": 8,
    "equipo_esperado": "",
    "solo_tarea": False,
    "estado_cada_min": 0,
    "campanas": True
}

# Nombres de los dos archivos del puente de Dropbox
CARPETA_PUENTE = "Flash_Acarreo_Data"        # se busca sola dentro de Dropbox
REMOTO_ORDEN  = "flash_orden.json"           # tu   -> bascula  (ordenes)
REMOTO_ESTADO = "flash_estado.json"          # bascula -> tu    (reporte)
REMOTO_NUEVO  = "flash_agente_nuevo.py"      # tu   -> bascula  (version nueva)
ARCHIVO_ORDENES = os.path.join(BASE, "ordenes_hechas.txt")
ARCHIVO_YOMISMO = os.path.abspath(__file__)
ARCHIVO_RESPALDO = os.path.join(BASE, "flash_agente.bak")
ARCHIVO_ACTUALIZACIONES = os.path.join(BASE, "actualizaciones.txt")
ARCHIVO_EXCEL = os.path.join(BASE, "excel_2025.json")   # V4.3
EXCEL_MAX = 10 * 1024 * 1024
EXCEL = {}            # fecha aaaa-mm-dd -> [registros de Excel]

# Huellas que debe tener un candidato para que lo aceptemos como agente
# legitimo. Evita que un archivo cualquiera termine reemplazando al agente.
FIRMA_AGENTE = ("def main(", "def leer_webservice(", "CARPETA_PUENTE", "VERSION")
TAM_MIN_AGENTE = 5 * 1024
TAM_MAX_AGENTE = 2 * 1024 * 1024

# Lo que NUNCA se acepta de una orden remota:
#   carpeta_dropbox / orden_url  -> para no poder quedar incomunicado ni
#                                   secuestrado hacia otra direccion
#   firebase_auth                -> una credencial se pone en persona, en
#                                   config.json, y nunca viaja por un canal
#                                   que otros puedan leer
NO_REMOTO = ("carpeta_dropbox", "orden_url", "firebase_auth")

# Productos que van a la planta. Todo lo demas cae en el bloque de no-planta.
# Se comparan sin acentos ni signos y por prefijo, asi que "JAL", "JALES" y
# "Jal de presa" caen todos en planta.
PRODUCTOS_PLANTA = ("mineral", "jal")

MESES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "set": 9, "oct": 10, "nov": 11, "dic": 12,
    "jan": 1, "apr": 4, "aug": 8, "dec": 12
}


# --------------------------------------------------------------------------
# Bitacora
# --------------------------------------------------------------------------

def log(msg, nivel="INFO"):
    linea = "%s [%s] %s" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), nivel, msg)
    try:
        if os.path.exists(ARCHIVO_LOG) and os.path.getsize(ARCHIVO_LOG) > LOG_MAX_BYTES:
            viejo = ARCHIVO_LOG + ".1"
            if os.path.exists(viejo):
                os.remove(viejo)
            os.rename(ARCHIVO_LOG, viejo)
        with open(ARCHIVO_LOG, "a", encoding="utf-8") as f:
            f.write(linea + "\n")
    except Exception:
        pass
    if CFG.get("verbose") or "--verbose" in sys.argv:
        try:
            print(linea)
        except Exception:
            pass


# --------------------------------------------------------------------------
# Configuracion
# --------------------------------------------------------------------------

INSTALADO = [False]

def cargar_config():
    """Solo se considera una instalacion de verdad si YA hay un config.json
    al lado. Antes, si no lo encontraba, lo creaba con valores por defecto
    -- comodo, pero convertia cualquier copia suelta en un agente a medias:
    bastaba un doble clic en Descargas para que encontrara Dropbox y
    machacara flash_estado.json con un reporte vacio, y tu vieras "0 boletos,
    sin senal" creyendo que se cayo la bascula.

    Ahora una copia suelta no crea nada, no escribe nada y se sale."""
    cfg = dict(CONFIG_DEFAULT)
    if not os.path.exists(ARCHIVO_CONFIG):
        return cfg
    try:
        with open(ARCHIVO_CONFIG, "r", encoding="utf-8") as f:
            cfg.update(json.load(f))
        INSTALADO[0] = True
    except Exception as e:
        print("No se pudo leer config.json: %s" % e)
    return cfg


CFG = cargar_config()


# --------------------------------------------------------------------------
# Normalizacion de datos del WebService
# --------------------------------------------------------------------------

def _clave(s):
    """Normaliza un nombre de campo: sin acentos, sin signos, minusculas."""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", s.lower())


# Mapeo tolerante. Aguanta las etiquetas de hoy (FecHora_PP, Neto_U_Pr truncado)
# y tambien etiquetas limpias si algun dia se renombran en RevueltaSIP.
REGLAS = [
    ("boleto",      ["boleto", "folio", "ticket"]),
    ("entrada",     ["fechorapp", "fechapp", "fechahorapp", "entrada",
                     "fechahora1a", "primerapesada"]),
    ("salida",      ["fechorasp", "fechasp", "fechahorasp", "salida",
                     "fechahora2a", "segundapesada"]),
    ("placas",      ["placas", "placa", "nocamion", "numerocamion", "unidad", "camion"]),
    ("chofer",      ["chofer", "conductor", "operadorcamion"]),
    ("pesador",     ["nombreoperador", "operador", "pesador"]),
    ("clase",       ["nombreusuario", "usuario", "clase", "clasematerial"]),
    ("procedencia", ["nombreempresa", "empresa", "procedencia", "origen"]),
    ("producto",    ["nombreproducto", "producto", "contenido"]),
    ("bruto",       ["brutouprim", "brutounidadprim", "pesobruto", "bruto"]),
    ("tara",        ["tarauprim", "tarounidadprim", "tara"]),
    ("neto",        ["netoupr", "netouprim", "netounidadprim", "pesoneto", "neto"]),
    ("peso",        ["pesopp", "pesounidadprim", "pesouprim", "peso"]),
    # V4.7: el "Dato adicional" de Revuelta se usa para el folio de la tarja
    ("tarja",       ["adicion", "adicional", "datoadicional", "foliotarja", "tarja"]),
    ("bascula_pp",  ["basculapp", "bascula1a"]),
    ("bascula_sp",  ["basculasp", "bascula2a"]),
]

def mapear(registro):
    """Traduce las llaves del JSON de RevueltaSIP a nombres estables.

    RevueltaSIP le agrega sufijos a las etiquetas y a veces las trunca:
    lo que capturas como 'Boleto' puede salir 'Boleto_PP', y 'Neto U Prim'
    sale 'Neto_U_Pr'. Por eso la busqueda es en tres pasadas -- exacta,
    por prefijo y por contenido -- y cada llave del JSON se consume una
    sola vez, para que un campo no le robe el lugar a otro.
    """
    origen = {_clave(k): v for k, v in registro.items()}
    salida = {}
    usadas = set()

    def buscar(candidatos, prueba):
        for cand in candidatos:
            for k in origen:
                if k in usadas:
                    continue
                if prueba(k, cand):
                    return k
        return None

    pasadas = [
        lambda k, c: k == c,                        # exacta
        lambda k, c: k.startswith(c),               # 'boletopp' <- 'boleto'
        lambda k, c: c.startswith(k) and len(k) >= 4,   # 'netoupr' <- 'netouprim'
        lambda k, c: c in k,                        # contiene
    ]

    for prueba in pasadas:
        for destino, candidatos in REGLAS:
            if destino in salida:
                continue
            k = buscar(candidatos, prueba)
            if k is not None:
                salida[destino] = origen[k]
                usadas.add(k)

    extras = {k: v for k, v in origen.items() if k not in usadas}
    if extras:
        salida["_extras"] = extras
    return salida


def a_numero(valor):
    """'25,700' -> 25700 . Devuelve None si no se puede."""
    if valor is None:
        return None
    if isinstance(valor, (int, float)):
        return valor
    s = re.sub(r"[^0-9\.\-]", "", str(valor))
    if s in ("", "-", "."):
        return None
    try:
        n = float(s)
        return int(n) if n == int(n) else n
    except Exception:
        return None


def a_fecha(valor):
    """'15/Sep/2026 07:51 am' -> '2026-09-15T07:51:00' . None si no se puede."""
    if not valor:
        return None
    s = str(valor).strip()
    m = re.match(
        r"(\d{1,2})[/\-]([A-Za-zÁ-úá-ú]{3,})[/\-](\d{4})"
        r"(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*([APap]\.?\s*[Mm]\.?)?)?",
        s)
    if not m:
        try:
            return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").isoformat()
        except Exception:
            return None
    dia = int(m.group(1))
    mes_txt = _clave(m.group(2))[:3]
    mes = MESES.get(mes_txt)
    if not mes:
        return None
    anio = int(m.group(3))
    hora = int(m.group(4) or 0)
    minu = int(m.group(5) or 0)
    seg = int(m.group(6) or 0)
    ampm = (m.group(7) or "").replace(".", "").replace(" ", "").lower()
    if ampm == "pm" and hora < 12:
        hora += 12
    if ampm == "am" and hora == 12:
        hora = 0
    try:
        return datetime(anio, mes, dia, hora, minu, seg).isoformat()
    except Exception:
        return None


def limpiar_texto(valor):
    if valor is None:
        return None
    return re.sub(r"\s+", " ", str(valor)).strip() or None


def normalizar(registro, abierto=False):
    """Un registro crudo del WebService -> el registro que guardamos."""
    r = mapear(registro)

    boleto = a_numero(r.get("boleto"))
    if boleto is None:
        return None

    bruto = a_numero(r.get("bruto"))
    tara = a_numero(r.get("tara"))
    neto_rep = a_numero(r.get("neto"))
    peso1 = a_numero(r.get("peso"))

    # El neto se CALCULA, no se confia en la columna: a veces viene vacia.
    neto_calc = None
    if bruto is not None and tara is not None:
        neto_calc = bruto - tara

    neto = neto_calc if neto_calc is not None else neto_rep

    entrada = a_fecha(r.get("entrada"))
    salida = a_fecha(r.get("salida"))

    minutos = None
    if entrada and salida:
        try:
            d = datetime.fromisoformat(salida) - datetime.fromisoformat(entrada)
            m = int(d.total_seconds() // 60)
            minutos = m if 0 <= m < 60 * 24 else None
        except Exception:
            pass

    procedencia = limpiar_texto(r.get("procedencia"))

    # V4.7: folio de la tarja (solo el numero, como viene impreso) y si la
    # pesada se tecleo a mano ("M") en vez de leerse de la bascula.
    tarja = limpiar_texto(r.get("tarja"))
    if tarja and re.fullmatch(r"[0-9 \-]+", tarja):
        tarja = re.sub(r"\D", "", tarja) or None
    pp = (limpiar_texto(r.get("bascula_pp")) or "").upper()
    sp = (limpiar_texto(r.get("bascula_sp")) or "").upper()
    pesada = None
    if pp or sp:
        m = ("1" if pp == "M" else "") + ("2" if sp == "M" else "")
        pesada = ("M" + m) if m else "N"

    reg = {
        "boleto": int(boleto),
        "entrada": entrada,
        "salida": salida,
        "minutos_bascula": minutos,
        "placas": limpiar_texto(r.get("placas")),
        "chofer": limpiar_texto(r.get("chofer")),
        "pesador": limpiar_texto(r.get("pesador")),
        "clase": limpiar_texto(r.get("clase")),
        "procedencia": procedencia,
        "grupo": agrupar(procedencia),
        "producto": limpiar_texto(r.get("producto")),
        "tarja": tarja,
        "pesada": pesada,
        "bruto": bruto,
        "tara": tara,
        "neto": neto,
        "neto_reportado": neto_rep,
        "abierto": bool(abierto),
        "origen_dato": "webservice",
        "agente": VERSION,
        "capturado_en": datetime.now().isoformat(timespec="seconds"),
    }
    if abierto and peso1 is not None:
        reg["peso_entrada"] = peso1

    # Banderas de calidad: se marcan, no se corrigen.
    alertas = []
    if neto_calc is not None and neto_rep is not None and neto_calc != neto_rep:
        alertas.append("neto_no_cuadra")
    if not abierto and neto is None:
        alertas.append("sin_neto")
    if bruto is not None and tara is not None and tara >= bruto:
        alertas.append("tara_mayor_que_bruto")
    if alertas:
        reg["alertas"] = alertas

    return reg


def agrupar(procedencia):
    """Realito Cooperativa + Realito Ejido Algarrobal = una sola procedencia
    en el resumen. El detalle conserva la razon social exacta."""
    if not procedencia:
        return None
    p = _clave(procedencia)
    if p.startswith("realito"):
        return "EL REALITO"
    return procedencia.strip().upper()


# --------------------------------------------------------------------------
# Lectura de los WebServices (LOCAL, nunca depende de internet)
# --------------------------------------------------------------------------

def leer_webservice(nombre, desde, hasta):
    url = "%s/%s/%s/%s" % (
        CFG["ws_base"].rstrip("/"), nombre,
        urllib.parse.quote(desde, safe=":"), urllib.parse.quote(hasta, safe=":"))
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=CFG["timeout_ws"]) as resp:
            crudo = resp.read()
    except Exception as e:
        log("WS %s no respondio: %s" % (nombre, e), "WARN")
        return None

    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            texto = crudo.decode(enc)
            break
        except Exception:
            texto = None
    if texto is None:
        log("WS %s: no se pudo decodificar" % nombre, "ERROR")
        return None

    texto = texto.strip()
    if not texto.startswith("[") and not texto.startswith("{"):
        log("WS %s no devolvio JSON. Revisa que 'Tipo servicio' este en JSON." % nombre, "ERROR")
        return None
    try:
        datos = json.loads(texto)
    except Exception as e:
        log("WS %s: JSON invalido: %s" % (nombre, e), "ERROR")
        return None
    if isinstance(datos, dict):
        datos = [datos]
    return datos


# --------------------------------------------------------------------------
# Firebase (lo unico que necesita internet)
# --------------------------------------------------------------------------

def firebase_patch(rutas):
    """Manda MUCHOS registros en UNA sola peticion.

    Firebase acepta una actualizacion multi-ruta: un PATCH en la raiz cuyo
    cuerpo lleva las rutas como llaves. Las reglas de seguridad se evaluan
    en cada ruta final, igual que si fueran escrituras sueltas -- asi que
    esto es tan seguro como mandarlas una por una, pero cientos de veces
    mas rapido. Mandar 45 boletos deja de ser 45 viajes de ida y vuelta
    a internet y pasa a ser uno.
    """
    url = "%s/.json" % CFG["firebase_url"].rstrip("/")
    if CFG.get("firebase_auth"):
        url += "?auth=" + CFG["firebase_auth"]
    cuerpo = json.dumps(rutas, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url, data=cuerpo, method="PATCH",
        headers={"Content-Type": "application/json; charset=utf-8"})
    with urllib.request.urlopen(req, timeout=max(30, CFG["timeout_firebase"])) as resp:
        return resp.status in (200, 204)


def firebase_get(ruta):
    """Lectura simple de Firebase. Se usa solo para /campanas.

    Falla en silencio a proposito: si no hay internet o la rama no existe,
    devuelve None y el agente sigue su corrida normal sin lotes. Nunca
    detiene el envio de boletos por esto.
    """
    url = "%s/%s.json" % (CFG["firebase_url"].rstrip("/"), ruta.strip("/"))
    if CFG.get("firebase_auth"):
        url += "?auth=" + CFG["firebase_auth"]
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=CFG["timeout_firebase"]) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log("No se pudo leer %s de Firebase: %s" % (ruta, e), "WARN")
        return None


def firebase_put(ruta, payload):
    url = "%s/%s.json" % (CFG["firebase_url"].rstrip("/"), ruta.strip("/"))
    if CFG.get("firebase_auth"):
        url += "?auth=" + CFG["firebase_auth"]
    cuerpo = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url, data=cuerpo, method="PUT",
        headers={"Content-Type": "application/json; charset=utf-8"})
    with urllib.request.urlopen(req, timeout=CFG["timeout_firebase"]) as resp:
        return resp.status in (200, 204)


def encolar(ruta, payload):
    try:
        with open(ARCHIVO_COLA, "a", encoding="utf-8") as f:
            f.write(json.dumps({"ruta": ruta, "payload": payload},
                               ensure_ascii=False) + "\n")
    except Exception as e:
        log("No se pudo encolar: %s" % e, "ERROR")


def vaciar_cola():
    if RED_CAIDA[0] or not os.path.exists(ARCHIVO_COLA):
        return 0
    try:
        with open(ARCHIVO_COLA, "r", encoding="utf-8") as f:
            lineas = [l for l in f.read().splitlines() if l.strip()]
    except Exception:
        return 0
    if not lineas:
        return 0
    if len(lineas) > COLA_MAX_LINEAS:
        lineas = lineas[-COLA_MAX_LINEAS:]
    pendientes = []
    enviados = 0
    i = 0
    while i < len(lineas):
        trozo = lineas[i:i + LOTE_MAX]
        paquete = {}
        for l in trozo:
            try:
                it = json.loads(l)
                paquete[it["ruta"].strip("/")] = it["payload"]
            except Exception:
                pass
        if not paquete:
            i += len(trozo); continue
        try:
            firebase_patch(paquete)
            enviados += len(paquete)
            i += len(trozo)
        except urllib.error.HTTPError as e:
            log("Firebase rechazo un lote de la cola (HTTP %s)." % e.code, "WARN")
            pendientes.extend(lineas[i:])
            break
        except Exception as e:
            RED_CAIDA[0] = True
            log("La red no responde al vaciar la cola (%s). Se deja para "
                "la proxima corrida." % e, "WARN")
            pendientes.extend(lineas[i:])
            break
    try:
        if pendientes:
            with open(ARCHIVO_COLA, "w", encoding="utf-8") as f:
                f.write("\n".join(pendientes) + "\n")
        elif os.path.exists(ARCHIVO_COLA):
            os.remove(ARCHIVO_COLA)
    except Exception:
        pass
    if enviados:
        log("Cola offline: %d registros enviados, %d pendientes"
            % (enviados, len(pendientes)))
    return enviados


LOTE = {}          # lo que se va a mandar junto
ENVIADOS = [0, 0]  # [registros, peticiones]
RED_CAIDA = [False]  # una vez que se cae, no se vuelve a intentar en esta corrida


def enviar(ruta, payload):
    """No manda todavia: apunta. Se manda todo junto en vaciar_lote()."""
    if CFG.get("modo_observacion"):
        return "observacion"
    LOTE[ruta.strip("/")] = payload
    if len(LOTE) >= LOTE_MAX:
        vaciar_lote()
    return "en lote"


def _encolar_todo(paquete):
    for ruta, payload in paquete.items():
        encolar(ruta, payload)


def vaciar_lote():
    """Manda lo acumulado en una sola peticion.

    Si falla, la reaccion depende de POR QUE fallo, y esa distincion es
    la que protege a la PC de bascula:

      - Firebase contesto y rechazo (un codigo HTTP): el lote trae algun
        registro malo. Vale la pena mandarlos uno por uno para aislarlo,
        porque cada intento es rapido: hay servidor del otro lado.

      - Firebase NO contesto (red caida, DNS, timeout): insistir uno por
        uno seria esperar el timeout cientos o miles de veces. Con una
        carga historica encima eso deja procesos colgados durante horas,
        y la tarea programada dispara otro cada 3 minutos. Se encola todo
        de golpe, se marca la red como caida y esta corrida ya no vuelve
        a intentar. Lo encolado se manda solo cuando la red regrese.
    """
    if not LOTE:
        return
    paquete = dict(LOTE)
    LOTE.clear()

    if RED_CAIDA[0]:
        _encolar_todo(paquete)
        return

    try:
        firebase_patch(paquete)
        ENVIADOS[0] += len(paquete)
        ENVIADOS[1] += 1
        return
    except urllib.error.HTTPError as e:
        log("Firebase contesto pero rechazo el lote (HTTP %s). "
            "Se mandan uno por uno para aislar el registro malo." % e.code, "WARN")
    except Exception as e:
        RED_CAIDA[0] = True
        log("Firebase no contesta (%s). Se encolan %d registros completos "
            "y esta corrida ya no insiste. Se mandaran cuando vuelva la red."
            % (e, len(paquete)), "WARN")
        _encolar_todo(paquete)
        return

    # Lista ordenada, para poder encolar EXACTAMENTE lo que falta si la red
    # se cae a media tanda -- sin reencolar lo que ya se fue bien.
    items = list(paquete.items())
    for i, (ruta, payload) in enumerate(items):
        try:
            firebase_put(ruta, payload)
            ENVIADOS[0] += 1
            ENVIADOS[1] += 1
        except urllib.error.HTTPError as e2:
            log("Firebase rechazo %s (HTTP %s). Se encola." % (ruta, e2.code), "WARN")
            encolar(ruta, payload)
        except Exception as e2:
            RED_CAIDA[0] = True
            log("Se perdio la red a medio envio (%s). Se encolan los %d "
                "que faltaban, sin insistir." % (e2, len(items) - i), "WARN")
            for r2, p2 in items[i:]:
                encolar(r2, p2)
            return



# --------------------------------------------------------------------------
# Puente de Dropbox: recibe ordenes y reporta estado sin acceso remoto
# --------------------------------------------------------------------------

def raiz_dropbox():
    """Encuentra donde tiene Dropbox su carpeta en ESTA maquina.

    Dropbox deja un info.json con la ruta exacta, asi que no hay que
    adivinarla ni pedirsela a nadie: sirve igual en tu equipo y en el de
    la bascula, aunque cada uno la tenga en un lugar distinto.
    """
    for base in (os.environ.get("LOCALAPPDATA"), os.environ.get("APPDATA")):
        if not base:
            continue
        info = os.path.join(base, "Dropbox", "info.json")
        try:
            if os.path.exists(info):
                with open(info, "r", encoding="utf-8") as f:
                    d = json.load(f)
                for cuenta in ("personal", "business"):
                    ruta = (d.get(cuenta) or {}).get("path")
                    if ruta and os.path.isdir(ruta):
                        return ruta
        except Exception:
            pass
    perfil = os.environ.get("USERPROFILE") or os.path.expanduser("~")
    for cand in (os.path.join(perfil, "Dropbox"),
                 os.path.join(perfil, "Desktop", "Dropbox")):
        if os.path.isdir(cand):
            return cand
    return None


_CACHE_PUENTE = [None, False]

def carpeta_puente():
    """Localiza Flash_Acarreo_Data dentro de Dropbox. Se busca una sola vez."""
    if _CACHE_PUENTE[1]:
        return _CACHE_PUENTE[0]
    _CACHE_PUENTE[1] = True

    fija = (CFG.get("carpeta_dropbox") or "").strip()
    if fija and os.path.isdir(fija):
        _CACHE_PUENTE[0] = fija
        return fija

    raiz = raiz_dropbox()
    if not raiz:
        log("No se encontro Dropbox en este equipo. El puente queda apagado.")
        return None
    for actual, dirs, _ in os.walk(raiz):
        if actual.count(os.sep) - raiz.count(os.sep) > 4:
            dirs[:] = []
            continue
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        if CARPETA_PUENTE in dirs:
            _CACHE_PUENTE[0] = os.path.join(actual, CARPETA_PUENTE)
            log("Puente de Dropbox: %s" % _CACHE_PUENTE[0])
            return _CACHE_PUENTE[0]
    log("No se encontro la carpeta %s dentro de Dropbox." % CARPETA_PUENTE, "WARN")
    return None


def _orden_de_dropbox():
    """La orden que quedo en la carpeta compartida. Es la linea de respaldo:
    si no hay internet, esta es la que manda."""
    carpeta = carpeta_puente()
    if not carpeta:
        return None
    ruta = os.path.join(carpeta, REMOTO_ORDEN)
    if not os.path.exists(ruta):
        return None
    try:
        with open(ruta, "r", encoding="utf-8-sig") as f:
            orden = json.load(f)
        return orden if isinstance(orden, dict) else None
    except Exception as e:
        log("La orden de Dropbox vino rota (%s). Se ignora." % e, "WARN")
        return None


def _orden_de_internet():
    """La orden publicada en GitHub. Es la linea principal: tu la controlas
    sin depender de permisos de escritura en carpetas de nadie mas."""
    url = (CFG.get("orden_url") or "").strip()
    if not url:
        return None
    # El CDN de GitHub cachea; esto cambia la direccion cada 2 minutos
    # para que una orden nueva no se quede atorada media hora.
    sep = "&" if "?" in url else "?"
    url = "%s%st=%d" % (url, sep, int(time.time() // 120))
    try:
        req = urllib.request.Request(url, headers={
            "Cache-Control": "no-cache",
            "User-Agent": "FlashAcarreo/%s" % VERSION})
        with urllib.request.urlopen(req, timeout=int(CFG.get("timeout_orden", 8))) as r:
            if getattr(r, "status", 200) != 200:
                return None
            crudo = r.read(200000).decode("utf-8-sig", "replace")
        orden = json.loads(crudo)
        return orden if isinstance(orden, dict) else None
    except Exception as e:
        # Sin internet no pasa nada: se usa la de Dropbox y ya.
        if CFG.get("verbose"):
            log("No se pudo leer la orden por internet (%s)." % e)
        return None


def leer_orden():
    """Busca la orden por DOS caminos distintos y se queda con la de internet.

    Internet (GitHub) es el camino principal porque tu lo controlas solo.
    Dropbox es el respaldo: lo que se haya dejado ahi en persona sigue
    valiendo si la red se cae, que en bascula pasa seguido.

    'config' se aplica en cada corrida. 'carga_dias' se ejecuta UNA sola
    vez por cada 'id' distinto, para que no se repita cada tres minutos.
    """
    orden = _orden_de_dropbox() or {}
    fuente = "Dropbox" if orden else None

    remota = _orden_de_internet()
    if remota is not None:
        orden = remota
        fuente = "internet"

    if not orden:
        return {}

    cambios = []
    bloqueados = []
    for k, v in (orden.get("config") or {}).items():
        if k.startswith("_"):
            continue
        if k in NO_REMOTO:
            bloqueados.append(k)
            continue
        if CFG.get(k) != v:
            cambios.append("%s=%s" % (k, v))
        CFG[k] = v
    if cambios:
        log("Orden por %s aplicada: %s" % (fuente, ", ".join(cambios)))
    if bloqueados:
        log("La orden traia campos que NO se aceptan de forma remota "
            "y se ignoraron: %s" % ", ".join(bloqueados), "WARN")
    return orden


def orden_ya_hecha(ident):
    try:
        if os.path.exists(ARCHIVO_ORDENES):
            with open(ARCHIVO_ORDENES, "r", encoding="utf-8") as f:
                return str(ident) in f.read().splitlines()
    except Exception:
        pass
    return False


def marcar_orden(ident):
    try:
        with open(ARCHIVO_ORDENES, "a", encoding="utf-8") as f:
            f.write(str(ident) + "\n")
    except Exception:
        pass


def _huella(ruta):
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(65536), b""):
            h.update(bloque)
    return h.hexdigest()


# Veredictos que SI cierran el caso de un archivo: o ya se instalo, o el
# archivo mismo esta mal y no va a mejorar solo. Un "NO_COINCIDE" no entra
# aqui a proposito: eso no dice nada del archivo, solo que la orden y el
# archivo todavia no se han puesto de acuerdo, y eso se corrige y se reintenta.
VEREDICTOS_FINALES = ("APLICADA", "RECHAZADA")


def _ya_visto(huella):
    try:
        if not os.path.exists(ARCHIVO_ACTUALIZACIONES):
            return False
        with open(ARCHIVO_ACTUALIZACIONES, "r", encoding="utf-8") as f:
            for linea in f:
                partes = linea.rstrip("\n").split("\t")
                if len(partes) >= 3 and partes[1] == huella \
                        and partes[2] in VEREDICTOS_FINALES:
                    return True
    except Exception:
        pass
    return False


def _anotar(huella, veredicto, detalle=""):
    try:
        with open(ARCHIVO_ACTUALIZACIONES, "a", encoding="utf-8") as f:
            f.write("%s\t%s\t%s\t%s\n" % (
                datetime.now().isoformat(timespec="seconds"),
                huella, veredicto, detalle))
    except Exception:
        pass


def autoactualizar(orden):
    """Se reemplaza a si mismo con la version que dejes en Dropbox.

    Tu pones flash_agente_nuevo.py en la carpeta Flash_Acarreo_Data y el
    agente que vive en C:\\FlashAcarreo se actualiza solo en la siguiente
    corrida. Nadie en bascula tiene que copiar ni pegar nada nunca mas.

    Esto es seguro para el proceso que esta corriendo ahora mismo: Python
    ya leyo y compilo este archivo al arrancar, asi que cambiarlo en disco
    no altera la corrida en curso. La version nueva entra hasta la
    siguiente, tres minutos despues.

    Antes de reemplazar nada se revisa que el candidato:
      1) tenga un tamano razonable,
      2) COMPILE sin errores de sintaxis,
      3) traiga las firmas de un agente de verdad.
    Si falla cualquiera de las tres, no se toca nada y queda anotado.
    El archivo anterior siempre queda en flash_agente.bak.
    """
    modo = CFG.get("actualizacion_automatica")
    if modo in (False, None, "", "no"):
        return None

    # --- LLAVE 1: la orden tiene que DECLARAR la huella exacta -------------
    # Sin huella declarada no hay actualizacion. Nunca. Esto es lo que hace
    # que dejar un .py en una carpeta compartida no baste por si solo.
    pedido = (orden or {}).get("actualizar") or {}
    esperada = str(pedido.get("sha256") or "").strip().lower()
    if not esperada:
        return None
    if len(esperada) != 64 or not re.fullmatch(r"[0-9a-f]{64}", esperada):
        log("La orden trae un sha256 que no tiene forma de sha256. Se ignora.", "WARN")
        return None
    if _ya_visto(esperada):
        return None

    # --- LLAVE 2: el archivo, por el camino que digas ----------------------
    bytes_nuevos = None
    de_donde = None
    if modo in (True, "dropbox", "ambos"):
        carpeta = carpeta_puente()
        if carpeta:
            candidato = os.path.join(carpeta, REMOTO_NUEVO)
            if os.path.exists(candidato):
                try:
                    with open(candidato, "rb") as f:
                        bytes_nuevos = f.read(TAM_MAX_AGENTE + 1)
                    de_donde = "Dropbox"
                except Exception as e:
                    log("No se pudo leer la version nueva de Dropbox: %s" % e, "WARN")
    if bytes_nuevos is None and modo in ("url", "ambos"):
        url = str(pedido.get("url") or "").strip()
        if url.startswith("https://"):
            try:
                req = urllib.request.Request(url, headers={
                    "Cache-Control": "no-cache",
                    "User-Agent": "FlashAcarreo/%s" % VERSION})
                with urllib.request.urlopen(req, timeout=int(CFG.get("timeout_orden", 8))) as r:
                    bytes_nuevos = r.read(TAM_MAX_AGENTE + 1)
                de_donde = "internet"
            except Exception as e:
                log("No se pudo bajar la version nueva (%s)." % e, "WARN")
    if bytes_nuevos is None:
        return None

    # La huella se calcula sobre lo que REALMENTE llego, no sobre lo que
    # el archivo dice de si mismo.
    huella = hashlib.sha256(bytes_nuevos).hexdigest()
    if huella != esperada:
        _anotar(huella, "NO_COINCIDE",
                "la orden pedia %s... ; se reintenta cuando cuadren" % esperada[:12])
        log("Version nueva RECHAZADA: la huella no coincide. "
            "Llego %s..., la orden pide %s...  No se toco nada."
            % (huella[:12], esperada[:12]), "WARN")
        return None

    # Ya somos esa version: silencio total.
    try:
        if huella == _huella(ARCHIVO_YOMISMO):
            return None
    except Exception:
        pass

    tam = len(bytes_nuevos)
    if not (TAM_MIN_AGENTE <= tam <= TAM_MAX_AGENTE):
        _anotar(huella, "RECHAZADA", "tamano fuera de rango: %d bytes" % tam)
        log("Version nueva rechazada: tamano raro (%d bytes)." % tam, "WARN")
        return None

    try:
        fuente = bytes_nuevos.decode("utf-8")
    except Exception as e:
        _anotar(huella, "RECHAZADA", "no es texto utf-8: %s" % e)
        return None

    faltantes = [s for s in FIRMA_AGENTE if s not in fuente]
    if faltantes:
        _anotar(huella, "RECHAZADA", "no parece el agente, falta: %s" % ", ".join(faltantes))
        log("Version nueva rechazada: no parece el agente Flash.", "WARN")
        return None

    try:
        compile(fuente, "flash_agente_nuevo.py", "exec")
    except SyntaxError as e:
        _anotar(huella, "RECHAZADA", "no compila: linea %s, %s" % (e.lineno, e.msg))
        log("Version nueva rechazada: no compila (linea %s: %s)." % (e.lineno, e.msg), "WARN")
        return None

    nueva = "?"
    m = re.search(r'^VERSION\s*=\s*["\']([^"\']+)', fuente, re.M)
    if m:
        nueva = m.group(1)

    try:
        shutil.copy2(ARCHIVO_YOMISMO, ARCHIVO_RESPALDO)
        tmp = ARCHIVO_YOMISMO + ".nuevo"
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            f.write(fuente)
        os.replace(tmp, ARCHIVO_YOMISMO)
    except Exception as e:
        _anotar(huella, "FALLO", str(e))
        log("No se pudo aplicar la version nueva (%s). Sigue la V%s." % (e, VERSION), "WARN")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return None

    _anotar(huella, "APLICADA", "V%s -> V%s por %s" % (VERSION, nueva, de_donde))
    log("ACTUALIZADO: V%s reemplazada por V%s desde %s. "
        "Entra en la siguiente corrida. Respaldo en flash_agente.bak."
        % (VERSION, nueva, de_donde))
    return nueva


# Lo que, si cambia, amerita avisar de inmediato aunque toque esperar.
CAMPOS_AVISO = ("agente", "modo", "boletos_procesados", "camiones_en_patio",
                "cola_pendiente", "ws_ok", "carga_historica", "version_pendiente")


def _vale_la_pena_escribir(ruta, estado):
    """Con estado_cada_min en 0 se escribe siempre, como hasta ahora.

    Con un numero, se escribe solo cada tantos minutos -- salvo que algo
    que de verdad importa haya cambiado, y entonces se escribe al momento.
    Esto existe porque Dropbox notifica cada cambio: escribiendo cada 3
    minutos son ~480 avisos al dia, y un aviso que llega siempre deja de
    ser un aviso. Con 15 minutos son ~30, y los que lleguen fuera de
    tiempo significan algo.
    """
    cada = int(CFG.get("estado_cada_min") or 0)
    if cada <= 0 or not os.path.exists(ruta):
        return True
    try:
        with open(ruta, "r", encoding="utf-8") as f:
            previo = json.load(f)
    except Exception:
        return True
    for k in CAMPOS_AVISO:
        if previo.get(k) != estado.get(k):
            return True
    try:
        antes = datetime.fromisoformat(previo.get("ultima_corrida"))
        return (datetime.now() - antes).total_seconds() >= cada * 60
    except Exception:
        return True


def escribir_estado(estado):
    """Deja flash_estado.json en Dropbox: la ventana a la bascula
    cuando no hay acceso remoto."""
    carpeta = carpeta_puente()
    if not carpeta:
        return
    try:
        cola = []
        try:
            with open(ARCHIVO_LOG, "r", encoding="utf-8") as f:
                cola = f.read().splitlines()[-25:]
        except Exception:
            pass
        estado = dict(estado)
        estado["bitacora_reciente"] = cola
        ruta = os.path.join(carpeta, REMOTO_ESTADO)
        if not _vale_la_pena_escribir(ruta, estado):
            return
        tmp = ruta + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(estado, f, indent=2, ensure_ascii=False)
        if os.path.exists(ruta):
            os.remove(ruta)
        os.rename(tmp, ruta)
    except Exception as e:
        log("No se pudo escribir el estado en Dropbox: %s" % e, "WARN")


def revisar_candados():
    """Devuelve el motivo por el que NO debe correr, o None si puede.

    Dos candados, los dos apagados de fabrica y encendidos por orden remota:

      equipo_esperado  Solo corre en esa maquina. Si copian la carpeta
                       completa a otra PC, ahi no hace nada.
      solo_tarea       Solo corre lanzado por la tarea programada. La tarea
                       usa pythonw.exe (sin ventana) y un doble clic usa
                       python.exe, asi que se distinguen sin tocar la tarea.
                       Con --manual se puede forzar a proposito.
    """
    esperado = str(CFG.get("equipo_esperado") or "").strip()
    if esperado:
        actual = (os.environ.get("COMPUTERNAME") or "").strip()
        if actual.upper() != esperado.upper():
            return ("Este agente esta anclado al equipo '%s' y aqui dice '%s'. "
                    "No hace nada." % (esperado, actual or "sin nombre"))

    if CFG.get("solo_tarea") and "--manual" not in sys.argv:
        exe = os.path.basename(sys.executable or "").lower()
        if not exe.startswith("pythonw"):
            return ("Este agente solo corre desde su tarea programada. "
                    "Para correrlo a proposito, agregale  --manual")
    return None


def es_planta(producto):
    """Lo que va a la planta se decide por PRODUCTO, no por procedencia.

    Antes el visor separaba con una lista de procedencias escrita a mano
    ('GRAVARENA', 'DIESEL'). Funcionaba de casualidad, porque esas dos son
    su propia procedencia. El dia que llegue gravarena facturada a nombre
    de una empresa real, se contaria como mineral y nadie se enteraria.

    Con el producto eso no pasa: mineral y jal van a planta, y CUALQUIER
    otra cosa -- incluida una que todavia no existe -- cae al bloque de
    no-planta con su nombre a la vista, nunca se traga en silencio.
    """
    p = _clave(producto or "")
    return any(p.startswith(x) for x in PRODUCTOS_PLANTA)


# --------------------------------------------------------------------------
# V4.7 - Boletos excluidos del Flash
# --------------------------------------------------------------------------
# Los pone el visor en /excluidos/{boleto} (permiso especial, con motivo y
# comentario). El agente solo los LEE. Un boleto excluido se sigue mandando
# a /boletos, marcado con "excluido", pero no cuenta en el resumen, en las
# campanas ni en el cuadre. En Revuelta no cambia nada.
ARCHIVO_EXCL = os.path.join(BASE, "excluidos_vistos.json")
EXCL = {}                 # boleto (int) -> {motivo, comentario, por, cuando, fecha}
EXCL_CAMPOS = ("motivo", "comentario", "por", "cuando", "fecha")


def _fb_leer(ruta):
    """Como firebase_get, pero distingue 'no existe' (True, None) de
    'no se pudo leer' (False, None)."""
    url = "%s/%s.json" % (CFG["firebase_url"].rstrip("/"), ruta.strip("/"))
    if CFG.get("firebase_auth"):
        url += "?auth=" + CFG["firebase_auth"]
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=CFG["timeout_firebase"]) as resp:
            return True, json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log("No se pudo leer %s de Firebase: %s" % (ruta, e), "WARN")
        return False, None


def _excl_previos():
    try:
        with open(ARCHIVO_EXCL, "r", encoding="utf-8") as f:
            d = json.load(f)
        return {int(k): v for k, v in (d.get("boletos") or {}).items()}
    except Exception:
        return {}


def guardar_excluidos_vistos():
    try:
        tmp = ARCHIVO_EXCL + ".nuevo"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"boletos": {str(k): v for k, v in EXCL.items()},
                       "guardado": datetime.now().isoformat(timespec="seconds")},
                      f, ensure_ascii=False)
        os.replace(tmp, ARCHIVO_EXCL)
    except Exception as e:
        log("No se pudo guardar excluidos_vistos.json (%s)" % e, "WARN")


def cargar_excluidos():
    """Lee /excluidos. Devuelve {fecha: [boletos]} con los dias que hay que
    recalcular porque alguien excluyo o restauro un boleto desde la ultima
    vez. Si Firebase no responde, se usa lo ultimo que se vio y no se
    recalcula nada."""
    global EXCL
    previo = _excl_previos()
    ok, datos = _fb_leer("excluidos")
    if not ok:
        EXCL = previo
        return None
    actual = {}
    for k, v in (_como_dict(datos) or {}).items():
        b = _entero(k)
        if b is None or not isinstance(v, dict):
            continue
        actual[b] = {c: v.get(c) for c in EXCL_CAMPOS if v.get(c) is not None}
    EXCL = actual
    cambios = {}
    for b in set(actual) | set(previo):
        if (b in actual) == (b in previo):
            continue
        f = str((actual.get(b) or previo.get(b) or {}).get("fecha") or "")[:10]
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", f):
            log("Excluido %s sin fecha valida; no se puede recalcular su dia." % b, "WARN")
            f = ""
        cambios.setdefault(f, []).append(b)
    return cambios


def excluido(boleto):
    return EXCL.get(_entero(boleto))


# --------------------------------------------------------------------------
# Campanas (lotes)
# --------------------------------------------------------------------------
# Una campana no guarda sus camiones: guarda DONDE EMPIEZA. El lote 24 de
# Cerro Blanco es "todos los boletos de Cerro Blanco del 27-sep 07:15 en
# adelante, hasta que empiece el 25". Por eso el arranque se guarda como un
# MOMENTO y no como un folio: el folio es de toda la bascula y es casualidad
# que caiga justo en el primer camion de una procedencia.
#
# Aqui no se decide nada. Lo que se decide vive en /campanas, que lo escribe
# el panel. Esto nada mas aplica la regla y deja el numero puesto.

CAMPANAS = {}          # clave de grupo -> [lote, ...] del numero mas alto al mas bajo
EXCLUIDOS = {}         # V4.2: clave de grupo -> {boleto, ...} que no cuentan en ninguna campana
CAMPANA_DIAS_ATRAS = 120   # hasta donde se mira el resumen para sumar un lote
_RESUMEN_PREVIO = {}       # cache de una sola corrida


def clave_fb(texto):
    """Firebase no acepta . # $ [ ] / en una llave. El visor usa la misma regla."""
    t = str(texto or "SIN PROCEDENCIA")
    for c in ".#$[]/":
        t = t.replace(c, "_")
    return t


def _entero(v):
    try:
        if v is None or v == "":
            return None
        return int(float(v))
    except Exception:
        return None


def _como_dict(v):
    """Firebase convierte {"1":..,"2":..} en [null, .., ..]. Se regresa a dict."""
    if isinstance(v, list):
        return {str(i): x for i, x in enumerate(v) if x is not None}
    return v if isinstance(v, dict) else None


def cargar_campanas():
    """Trae /campanas y la deja lista para consultar. Devuelve cuantos lotes
    quedaron cargados. Cualquier cosa rara se ignora en silencio: mas vale
    un boleto sin lote que una corrida detenida.

    Cada lote arranca por boleto (desde_boleto, V4.1) o por momento (desde,
    V4.0). Puede traer tope_kg y siguiente_boleto."""
    global CAMPANAS, EXCLUIDOS
    CAMPANAS = {}
    EXCLUIDOS = {}
    _RESUMEN_PREVIO.clear()
    if not CFG.get("campanas", True):
        return 0
    datos = firebase_get("campanas")
    if not isinstance(datos, dict):
        return 0
    total = 0
    for grupo, cuerpo in datos.items():
        if grupo.startswith("_") or not isinstance(cuerpo, dict):
            continue
        ex = _como_dict(cuerpo.get("excluidos"))
        if ex:
            EXCLUIDOS[grupo] = set(b for b in (_entero(k) for k in ex.keys()) if b is not None)
        # Firebase regresa una LISTA (no un diccionario) cuando las llaves son
        # numeros casi seguidos, p. ej. lotes 1..25. Se normaliza.
        lotes = _como_dict(cuerpo.get("lotes"))
        if not isinstance(lotes, dict):
            continue
        filas = []
        for clave, lote in lotes.items():
            if not isinstance(lote, dict):
                continue
            numero = _entero(lote.get("numero", clave))
            if numero is None:
                continue
            desde = lote.get("desde")
            if not (isinstance(desde, str) and len(desde) >= 10):
                desde = None
            desde_boleto = _entero(lote.get("desde_boleto"))
            if desde is None and desde_boleto is None:
                continue
            try:
                tope = float(lote.get("tope_kg")) if lote.get("tope_kg") not in (None, "") else None
            except Exception:
                tope = None
            filas.append({"numero": numero, "desde": desde, "desde_boleto": desde_boleto,
                          "tope_kg": tope if tope and tope > 0 else None,
                          "siguiente_boleto": _entero(lote.get("siguiente_boleto"))})
        if filas:
            filas.sort(key=lambda x: x["numero"], reverse=True)   # el mas nuevo, primero
            CAMPANAS[grupo] = filas
            total += len(filas)
    return total


def _pertenece(lote, entrada, boleto):
    if lote.get("desde_boleto") is not None:
        return boleto is not None and boleto >= lote["desde_boleto"]
    return bool(entrada) and entrada >= lote["desde"]


def lote_de(grupo, entrada, boleto=None):
    """A que lote pertenece este boleto. None si esa procedencia no lleva
    campanas, o si el camion es anterior a la primera."""
    gk = clave_fb(grupo)
    filas = CAMPANAS.get(gk)
    if not filas:
        return None
    if boleto is not None and _entero(boleto) in EXCLUIDOS.get(gk, ()):
        return None   # V4.2: excluido por el gestor (duplicado, tara 0, no es del flete...)
    if boleto is not None and _entero(boleto) in EXCL:
        return None   # V4.7: excluido del Flash
    for lote in filas:
        if _pertenece(lote, entrada, boleto):
            return lote["numero"]
    return None


def firebase_get_rango(ruta, desde, hasta):
    """Lectura por rango de llaves (fechas). None si no se pudo leer."""
    q = {"orderBy": '"$key"', "startAt": '"%s"' % desde, "endAt": '"%s"' % hasta}
    if CFG.get("firebase_auth"):
        q["auth"] = CFG["firebase_auth"]
    url = "%s/%s.json?%s" % (CFG["firebase_url"].rstrip("/"), ruta.strip("/"),
                             urllib.parse.urlencode(q))
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=CFG["timeout_firebase"]) as resp:
            datos = json.loads(resp.read().decode("utf-8"))
            return datos if isinstance(datos, dict) else {}
    except Exception as e:
        log("No se pudo leer el rango %s de %s: %s" % (ruta, desde, e), "WARN")
        return None


def _kg_previo(grupo_k, numero, inicio_ventana):
    """Kg de planta de ese lote en los dias ANTES de la ventana de esta
    corrida, tomados del resumen que ya esta escrito (por_lote). Los dias de
    la ventana se cuentan boleto por boleto, asi que no se suman dos veces."""
    if "datos" not in _RESUMEN_PREVIO:
        ini = datetime.strptime(inicio_ventana, "%Y-%m-%d")
        _RESUMEN_PREVIO["datos"] = firebase_get_rango(
            "resumen",
            (ini - timedelta(days=CAMPANA_DIAS_ATRAS)).strftime("%Y-%m-%d"),
            (ini - timedelta(days=1)).strftime("%Y-%m-%d"))
    datos = _RESUMEN_PREVIO["datos"]
    if datos is None:
        return None
    total = 0
    for dia in datos.values():
        try:
            g = _como_dict((((dia or {}).get("por_lote") or {}).get(grupo_k))) or {}
            total += (g.get(str(numero)) or {}).get("kg") or 0
        except Exception:
            pass
    return total


def _guardar_lote_auto(grupo_k, lote, motivo):
    ahora = datetime.now().isoformat(timespec="seconds")
    # Una sola peticion multi-ruta: el lote y su renglon de historial entran
    # juntos o no entra ninguno. (Las llaves llevan espacios: van en el
    # cuerpo, no en la URL.)
    ok = firebase_patch({
        "campanas/%s/lotes/%d" % (grupo_k, lote["numero"]): lote,
        "campanas/%s/historial/auto-%s-%d" % (grupo_k, ahora.replace(":", ""), lote["numero"]): {
            "cuando": ahora, "quien": "agente (automatico)",
            "que": "abre campana %d desde boleto %d" % (lote["numero"], lote["desde_boleto"]),
            "motivo": motivo},
    })
    if not ok:
        raise RuntimeError("Firebase no confirmo la escritura")


def avanzar_campanas(registros, inicio_ventana):
    """Cierra una campana al superar su tope y abre la siguiente.

    La siguiente arranca en (boleto que cruzo + 1): como el folio es de toda
    la bascula, eso es exactamente "el siguiente camion de esa procedencia".
    Si el gestor dejo siguiente_boleto, manda ese. Devuelve lo que hizo."""
    hechos = []
    for grupo_k in list(CAMPANAS.keys()):
        for _ in range(5):
            filas = CAMPANAS.get(grupo_k) or []
            if not filas:
                break
            cur = filas[0]
            nuevo, motivo = None, None
            if cur.get("siguiente_boleto"):
                nuevo = cur["siguiente_boleto"]
                motivo = "boleto de arranque indicado por el gestor"
            elif cur.get("tope_kg"):
                prev = _kg_previo(grupo_k, cur["numero"], inicio_ventana)
                if prev is None:
                    break          # sin el resumen no se decide nada
                propios = sorted(
                    [r for r in registros
                     if clave_fb(r.get("grupo")) == grupo_k and es_planta(r.get("producto"))
                     and lote_de(r.get("grupo"), r.get("entrada"), r.get("boleto")) == cur["numero"]],
                    key=lambda r: r["boleto"])
                if prev > cur["tope_kg"]:
                    if propios:
                        nuevo = propios[0]["boleto"]
                        motivo = "el tope ya estaba superado antes de esta corrida; revisar el arranque"
                else:
                    acum = prev
                    for r in propios:
                        acum += r.get("neto") or 0
                        if acum > cur["tope_kg"]:
                            nuevo = r["boleto"] + 1
                            motivo = "tope de %.3f t superado en el boleto %d (%.3f t)" % (
                                cur["tope_kg"] / 1000.0, r["boleto"], acum / 1000.0)
                            break
            if not nuevo:
                break
            lote = {"numero": cur["numero"] + 1, "desde_boleto": int(nuevo),
                    "tope_kg": cur.get("tope_kg"), "auto": True,
                    "firma": {"quien": "agente (automatico)",
                              "cuando": datetime.now().isoformat(timespec="seconds")}}
            if CFG.get("modo_observacion"):
                hechos.append("OBSERVACION: %s abriria campana %d desde boleto %d (%s)"
                              % (grupo_k, lote["numero"], lote["desde_boleto"], motivo))
                break
            try:
                _guardar_lote_auto(grupo_k, lote, motivo)
            except Exception as e:
                log("Campanas: no se pudo abrir la %d de %s (%s)" % (lote["numero"], grupo_k, e), "WARN")
                break
            filas.insert(0, {"numero": lote["numero"], "desde": None,
                             "desde_boleto": lote["desde_boleto"], "tope_kg": lote["tope_kg"],
                             "siguiente_boleto": None})
            hechos.append("%s: abre campana %d desde boleto %d (%s)"
                          % (grupo_k, lote["numero"], lote["desde_boleto"], motivo))
    return hechos


def _acumular(dest, llave, registro):
    a = dest.setdefault(llave or "SIN DATO", {"viajes": 0, "kg": 0})
    a["viajes"] += 1
    a["kg"] += (registro.get("neto") or 0)


def armar_resumen(fecha_k, registros):
    """El resumen del dia, ya desglosado para que el visor no tenga que
    bajarse los boletos de todo el ano para separar planta de no-planta."""
    planta = [r for r in registros if es_planta(r.get("producto"))]
    otros = [r for r in registros if not es_planta(r.get("producto"))]

    por_grupo, por_producto = {}, {}
    planta_grupo, otros_grupo = {}, {}
    for r in registros:
        _acumular(por_grupo, r.get("grupo") or "SIN PROCEDENCIA", r)
        _acumular(por_producto, (r.get("producto") or "SIN PRODUCTO").upper(), r)
    for r in planta:
        _acumular(planta_grupo, r.get("grupo") or "SIN PROCEDENCIA", r)

    # Desglose por lote: solo de lo que va a planta, que es lo que cuenta
    # para una campana. Queda vacio mientras nadie defina campanas.
    por_lote = {}
    for r in planta:
        numero = r.get("lote")
        if numero is None:
            continue
        grupo = clave_fb(r.get("grupo") or "SIN PROCEDENCIA")
        _acumular(por_lote.setdefault(grupo, {}), str(numero), r)
    for r in otros:
        _acumular(otros_grupo, r.get("grupo") or "SIN PROCEDENCIA", r)

    kg = lambda lista: sum(x.get("neto") or 0 for x in lista)
    return {
        "fecha": fecha_k,
        "viajes": len(registros),
        "kg": kg(registros),
        "toneladas": round(kg(registros) / 1000.0, 3),
        "planta": {"viajes": len(planta), "kg": kg(planta)},
        "no_planta": {"viajes": len(otros), "kg": kg(otros)},
        "por_producto": por_producto,
        "planta_por_grupo": planta_grupo,
        "no_planta_por_grupo": otros_grupo,
        "por_grupo": por_grupo,          # se conserva: el visor viejo lo usa
        "por_lote": por_lote,            # V4.0: {grupo: {numero_de_lote: {viajes, kg}}}
        "actualizado": datetime.now().isoformat(timespec="seconds"),
    }


def _leer_excel_local():
    EXCEL.clear()
    if not os.path.exists(ARCHIVO_EXCEL):
        return 0
    with open(ARCHIVO_EXCEL, "r", encoding="utf-8") as f:
        doc = json.load(f)
    n = 0
    for r in (doc.get("viajes") or []):
        if not isinstance(r, dict) or not r.get("fecha") or not r.get("boleto"):
            continue
        if not re.fullmatch(r"XLS[0-9A-Z]{2}-[0-9]+", str(r["boleto"])):
            continue
        r = dict(r)
        r["lote"] = None
        r["agente"] = VERSION
        EXCEL.setdefault(r["fecha"][:10], []).append(r)
        n += 1
    return n


def preparar_excel(orden):
    """V4.3. Baja (si hace falta) y lee el archivo de viajes de Excel.
    Devuelve el id de la carga si esta corrida debe hacerla, si no None.
    Nunca revienta: si algo falla, EXCEL queda vacio y todo sigue como V4.2."""
    pedido = (orden or {}).get("excel") or {}
    esperada = str(pedido.get("sha256") or "").strip().lower()
    url = str(pedido.get("url") or "").strip()
    ident = str(pedido.get("id") or "").strip()
    try:
        if re.fullmatch(r"[0-9a-f]{64}", esperada) and url.startswith("https://"):
            actual = _huella(ARCHIVO_EXCEL) if os.path.exists(ARCHIVO_EXCEL) else ""
            if actual != esperada:
                req = urllib.request.Request(url, headers={
                    "Cache-Control": "no-cache", "User-Agent": "FlashAcarreo/%s" % VERSION})
                with urllib.request.urlopen(req, timeout=60) as r:
                    datos = r.read(EXCEL_MAX + 1)
                huella = hashlib.sha256(datos).hexdigest()
                if huella != esperada or len(datos) > EXCEL_MAX:
                    log("Excel 2025: la huella no cuadra (%s... vs %s...). No se usa."
                        % (huella[:12], esperada[:12]), "WARN")
                    return None
                json.loads(datos.decode("utf-8"))      # que sea JSON valido
                tmp = ARCHIVO_EXCEL + ".nuevo"
                with open(tmp, "wb") as f:
                    f.write(datos)
                os.replace(tmp, ARCHIVO_EXCEL)
                log("Excel 2025: archivo nuevo guardado (%d bytes)." % len(datos))
        n = _leer_excel_local()
    except Exception as e:
        EXCEL.clear()
        log("Excel 2025: no se pudo preparar (%s). Corrida sin Excel." % e, "WARN")
        return None
    if n:
        log("Excel 2025: %d viajes en %d dias." % (n, len(EXCEL)))
    if n and ident and not orden_ya_hecha("excel-" + ident):
        return "excel-" + ident
    return None


# ==========================================================================
# V4.4 - ALERTAS: cuadre de bascula contra los Excel de fletes
# ==========================================================================
#
# Lee (SOLO LECTURA) los Excel de fletes que estan en la carpeta compartida
# de Dropbox, junto a Flash_Acarreo_Data:  21.-Bascula\2026\...\*.xlsx
# y los compara contra los boletos que ya estan en Firebase. Sube solo el
# resultado a /alertas. Nunca escribe, mueve ni bloquea un Excel: los abre
# como zip de lectura y en cuanto termina los suelta.
#
# Reglas (pura logica, siempre dan lo mismo con los mismos datos):
#   cuadra             mismo bruto y tara, fecha +-2 dias
#   peso distinto      mismo dia +-1, bruto o tara igual y el otro difiere
#                      poco (hasta 300 kg)
#   procedencia dist.  cuadra el peso, pero bascula lo registro en otra
#                      procedencia que la del Excel
#   en Excel, no en b. fila del Excel sin boleto
#   en bascula, no en  boleto de una procedencia que lleva Excel, dentro
#   Excel              de las fechas que ese Excel ya cubre, sin fila
#   boleto incompleto  tara 0 o neto 0 en bascula
#   neto mal calculado neto del Excel distinto de bruto - tara
#
# Todo va envuelto: si algo falla, el agente sigue igual que la V4.3.

import zipfile
import xml.etree.ElementTree as ET

ARCHIVO_CUADRE_CACHE = os.path.join(BASE, "cuadre_cache.json")
CUADRE_TOL_KG = 300            # diferencia maxima para "peso distinto"
CUADRE_DIAS = 2                # ventana de fechas para cuadrar
CUADRE_MAX_ALERTAS = 300       # tope de alertas que se suben
CUADRE_DUP_KG = 20             # duplicado: bruto casi igual (kg)
CUADRE_DUP_MIN = 180           # duplicado: a menos de 3 horas
CUADRE_TARJA_DIAS = 10         # V4.7: tarja repetida, a menos de 10 dias
#   Estimado con los Excel de 2026: en El Realito (40 viajes al dia, blocks
#   del 0001 al 5000 en paralelo) un folio se repite como minimo a los 21
#   dias; en las demas procedencias no se repite en menos de 77 dias. Con
#   10 dias y ademas un dato que amarre (chofer, placas o bruto) no se
#   espera ninguna alerta falsa.
_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_NSR = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"


def _sin_acentos(s):
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s).strip().upper()


def _col_idx(ref):
    """'AB12' -> 27 (base 0)."""
    n = 0
    for ch in ref:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - 64)
        else:
            break
    return n - 1


def _col_letra(i):
    s = ""
    i += 1
    while i:
        i, r = divmod(i - 1, 26)
        s = chr(65 + r) + s
    return s


def _xlsx_hojas(ruta):
    """Devuelve [(nombre_hoja, filas)] donde filas = [(numero_fila, {col: valor})].
    Solo libreria estandar. Lectura pura del zip."""
    hojas = []
    with zipfile.ZipFile(ruta) as z:
        nombres = set(z.namelist())
        compartidas = []
        if "xl/sharedStrings.xml" in nombres:
            with z.open("xl/sharedStrings.xml") as f:
                for _, el in ET.iterparse(f):
                    if el.tag == _NS + "si":
                        compartidas.append("".join(t.text or "" for t in el.iter(_NS + "t")))
                        el.clear()
        rels = {}
        if "xl/_rels/workbook.xml.rels" in nombres:
            raiz = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
            for r in raiz:
                destino = r.get("Target") or ""
                destino = destino.lstrip("/")
                if not destino.startswith("xl/"):
                    destino = "xl/" + destino
                rels[r.get("Id")] = destino
        libro = ET.fromstring(z.read("xl/workbook.xml"))
        for h in libro.iter(_NS + "sheet"):
            if (h.get("state") or "visible") != "visible":
                continue
            destino = rels.get(h.get(_NSR + "id"))
            if not destino or destino not in nombres:
                continue
            filas = []
            with z.open(destino) as f:
                for _, el in ET.iterparse(f):
                    if el.tag != _NS + "row":
                        continue
                    num = int(el.get("r") or (len(filas) + 1))
                    celdas = {}
                    for c in el.iter(_NS + "c"):
                        t = c.get("t")
                        v = c.find(_NS + "v")
                        if t == "inlineStr":
                            val = "".join(x.text or "" for x in c.iter(_NS + "t"))
                        elif v is None or v.text is None:
                            continue
                        elif t == "s":
                            try:
                                val = compartidas[int(v.text)]
                            except Exception:
                                continue
                        elif t in ("str", "e"):
                            val = v.text
                        elif t == "b":
                            val = v.text == "1"
                        else:
                            try:
                                val = float(v.text)
                            except Exception:
                                val = v.text
                        if val in ("", None):
                            continue
                        celdas[_col_idx(c.get("r") or "A")] = val
                    if celdas:
                        filas.append((num, celdas))
                    el.clear()
            hojas.append((h.get("name") or "?", filas))
    return hojas


def _fecha_excel(v):
    """Serial de Excel o texto -> 'aaaa-mm-dd' o None."""
    if isinstance(v, float):
        if 30000 < v < 80000:
            return (datetime(1899, 12, 30) + timedelta(days=int(v))).strftime("%Y-%m-%d")
        return None
    s = str(v or "").strip()
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        a, me, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    else:
        m = re.match(r"(\d{1,2})[/\-\. ]([A-Za-z]{3,}|\d{1,2})[/\-\. ](\d{2,4})", s)
        if not m:
            return None
        d = int(m.group(1))
        me = m.group(2)
        me = int(me) if me.isdigit() else MESES.get(_clave(me)[:3])
        a = int(m.group(3))
        a = a + 2000 if a < 100 else a
    try:
        return datetime(a, int(me), d).strftime("%Y-%m-%d")
    except Exception:
        return None


def _kg(v, en_ton=False):
    n = a_numero(v) if not isinstance(v, bool) else None
    if n is None:
        return None
    if 0 < n < 200:                    # toneladas -> kg (el encabezado no es confiable)
        n = n * 1000
    return int(round(n))


def _encabezado(filas):
    """Busca en las primeras 15 filas la que trae FECHA, BRUTO y TARA."""
    for num, celdas in filas[:15]:
        txt = {i: _sin_acentos(v) for i, v in celdas.items() if isinstance(v, str)}
        if not any("FECHA" in t for t in txt.values()):
            continue
        if not any("BRUTO" in t for t in txt.values()) or not any("TARA" in t for t in txt.values()):
            continue
        col = {}
        for i, t in sorted(txt.items()):
            if "FECHA" in t and "fecha" not in col:
                col["fecha"] = i
            elif "BRUTO" in t and "bruto" not in col:
                col["bruto"] = i
                col["bruto_ton"] = "TON" in t
            elif t.startswith("TARA") and "tara" not in col:
                col["tara"] = i
            elif "NETO" in t and "KG" in t and "neto" not in col:
                col["neto"] = i
            elif t == "TOTAL KG" and "neto" not in col:
                col["neto"] = i
            elif "CHOFER" in t and "chofer" not in col:
                col["chofer"] = i
            elif "CAMION" in t and "camion" not in col:
                col["camion"] = i
            elif "LOTE" in t and "lote" not in col:
                col["lote"] = i
            elif "PROCEDENCIA" in t and "procedencia" not in col:
                col["procedencia"] = i
            elif "CONTENIDO" in t and "contenido" not in col:
                col["contenido"] = i
            elif ("REV" in t or "BOLETO" in t) and "boleto" not in col:
                col["boleto"] = i          # V4.7: "FOLIO REV" = numero de boleto de Revuelta
            elif "FOLIO" in t and "folio" not in col:
                col["folio"] = i           # "FOLIO" o "FOLIO TARJA" = folio de la tarja
        if "fecha" in col and "bruto" in col and "tara" in col:
            return num, col
    return None, None


def _folio(v):
    """Folio de tarja como texto de 4 digitos ('0889'). None si no es un numero."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, float):
        if v != int(v) or v <= 0:
            return None
        return "%04d" % int(v)
    t = re.sub(r"[\s\-]", "", str(v))
    return t.zfill(4) if t.isdigit() and int(t) > 0 else None


def _tarja_num(v):
    try:
        n = int(re.sub(r"\D", "", str(v or "")) or 0)
        return n or None
    except Exception:
        return None


def leer_excel_fletes(ruta, desde, hasta):
    """Filas utiles de un Excel de fletes entre dos fechas (aaaa-mm-dd)."""
    salida = []
    for hoja, filas in _xlsx_hojas(ruta):
        num_h, col = _encabezado(filas)
        if not col:
            continue
        for num, c in filas:
            if num <= num_h:
                continue
            f = _fecha_excel(c.get(col["fecha"]))
            if not f or f < desde or f > hasta:
                continue
            bruto = _kg(c.get(col["bruto"]), col.get("bruto_ton"))
            tara = _kg(c.get(col["tara"]))
            if not bruto or not tara:
                continue
            neto = _kg(c.get(col["neto"])) if "neto" in col else None
            txt = lambda k: limpiar_texto(c.get(col[k])) if k in col and c.get(col[k]) is not None else None
            lote = c.get(col["lote"]) if "lote" in col else None
            if isinstance(lote, float):
                lote = int(lote) if lote == int(lote) else lote
            folio = _folio(c.get(col["folio"])) if "folio" in col else None
            boleto = _entero(a_numero(c.get(col["boleto"]))) if "boleto" in col else None
            salida.append({
                "folio": folio, "boleto": boleto if boleto and boleto > 1000 else None,
                "c_folio": _col_letra(col["folio"]) if "folio" in col else None,
                "hoja": hoja, "fila": num, "fecha": f, "bruto": bruto, "tara": tara,
                "neto": neto, "chofer": txt("chofer"), "camion": txt("camion"),
                "lote": lote, "procedencia": txt("procedencia"),
                "c_bruto": _col_letra(col["bruto"]), "c_tara": _col_letra(col["tara"]),
                "c_neto": _col_letra(col["neto"]) if "neto" in col else None,
            })
    return salida


def _carpetas_excel(desde, hasta):
    """Las carpetas de anio (2026, 2027...) junto a Flash_Acarreo_Data."""
    puente = carpeta_puente()
    if not puente:
        return None, "no se encontro la carpeta Flash_Acarreo_Data"
    raiz = os.path.dirname(puente)
    anios = range(int(desde[:4]), int(hasta[:4]) + 1)
    carpetas = [os.path.join(raiz, str(a)) for a in anios if os.path.isdir(os.path.join(raiz, str(a)))]
    if not carpetas:
        return None, "no encontre la carpeta %s en %s" % ("/".join(str(a) for a in anios), raiz)
    return (raiz, carpetas), None


def _familia(grupo):
    """VINATITA y VINATITA II URIQUE se comparan juntas (igual que el filtro
    del visor). El resto de las procedencias van tal cual."""
    g = _sin_acentos(grupo)
    if not g:
        return ""
    for k in (CFG.get("cuadre_familias") or {"VINATITA": "VINATITA"}).items():
        if g.startswith(k[0]):
            return k[1]
    return g


def _es_incompleto(b):
    return (not b.get("tara")) or (not b.get("neto")) or (b.get("neto") or 0) <= 0


def _llave(*partes):
    return hashlib.sha1("|".join(str(p) for p in partes).encode("utf-8")).hexdigest()[:12]


def _ton(kg):
    return round((kg or 0) / 1000.0, 3)


def cuadrar(excels, boletos, hoy_txt, excluidos=None):
    """El corazon. Sin red, sin disco: entra Excel y boletos, sale el reporte.

    excels    = {nombre_archivo: [filas de leer_excel_fletes]}
    boletos   = [registros de Firebase (cerrados), ya SIN los excluidos]
    excluidos = {boleto: {...}} solo para explicar en la pista
    """
    excluidos = excluidos or {}
    def dia(f):
        return datetime.strptime(f, "%Y-%m-%d")

    for b in boletos:
        b["_f"] = (b.get("entrada") or b.get("salida") or b.get("_fecha") or "")[:10]
    por_peso = {}
    for b in boletos:
        if b.get("bruto") and b.get("tara") is not None:
            por_peso.setdefault((int(b["bruto"]), int(b["tara"])), []).append(b)

    usados = {}          # boleto -> (archivo, fila)
    filas_todas = []     # (archivo, fila_dict, boleto_o_None, tipo)
    por_num = {b["boleto"]: b for b in boletos}

    # --- 0) V4.7: el Excel ya trae el numero de boleto (FOLIO REV). Si los
    # pesos le cuadran (o casi), ese es. Si no se parecen en nada, el numero
    # esta mal capturado y se cuadra por peso como siempre.
    directos = {}        # (archivo, fila) -> (boleto, tipo)
    for arch, filas in sorted(excels.items()):
        for r in filas:
            b = por_num.get(r.get("boleto"))
            if b is None or b["boleto"] in usados or not b["_f"] or not b.get("bruto"):
                continue
            if abs((dia(b["_f"]) - dia(r["fecha"])).days) > CUADRE_DIAS:
                continue
            db = abs(int(b["bruto"]) - r["bruto"])
            dt = abs(int(b.get("tara") or 0) - r["tara"])
            if db == 0 and dt == 0:
                directos[(arch, r["fila"])] = (b, "ok")
            elif db <= CUADRE_TOL_KG and dt <= CUADRE_TOL_KG:
                directos[(arch, r["fila"])] = (b, "peso")
            else:
                continue
            usados[b["boleto"]] = (arch, r["fila"])

    # --- 1) cuadre exacto, archivo por archivo (cada boleto una vez por archivo)
    for arch, filas in sorted(excels.items()):
        tomados = set()
        for r in sorted(filas, key=lambda x: (x["fecha"], x["fila"])):
            d0 = directos.get((arch, r["fila"]))
            if d0:
                tomados.add(d0[0]["boleto"])
                filas_todas.append([arch, r, d0[0], d0[1]])
                continue
            mejor = None
            for b in por_peso.get((r["bruto"], r["tara"]), []):
                if b["boleto"] in tomados or not b["_f"]:
                    continue
                if b["boleto"] in usados and usados[b["boleto"]][1] != r["fila"] and \
                        (usados[b["boleto"]][0], usados[b["boleto"]][1]) in directos:
                    continue          # ya lo tomo una fila por numero de boleto
                dd = abs((dia(b["_f"]) - dia(r["fecha"])).days)
                if dd <= CUADRE_DIAS and (mejor is None or dd < mejor[0]):
                    mejor = (dd, b)
            if mejor:
                b = mejor[1]
                tomados.add(b["boleto"])
                usados.setdefault(b["boleto"], (arch, r["fila"]))
                filas_todas.append([arch, r, b, "ok"])
            else:
                filas_todas.append([arch, r, None, None])

    # --- 2) peso distinto: lo que no cuadro, contra boletos que nadie tomo
    libres = [b for b in boletos if b["boleto"] not in usados and b.get("bruto")]
    for item in filas_todas:
        if item[2] is not None:
            continue
        r = item[1]
        mejor = None
        for b in libres:
            if b["boleto"] in usados or not b["_f"]:
                continue
            dd = abs((dia(b["_f"]) - dia(r["fecha"])).days)
            if dd > 1:
                continue
            db = abs(int(b["bruto"]) - r["bruto"])
            dt = abs(int(b.get("tara") or 0) - r["tara"])
            if (db == 0 and 0 < dt <= CUADRE_TOL_KG) or (dt == 0 and 0 < db <= CUADRE_TOL_KG):
                score = (dd, db + dt)
                if mejor is None or score < mejor[0]:
                    mejor = (score, b)
        if mejor:
            b = mejor[1]
            usados[b["boleto"]] = (item[0], r["fila"])
            item[2] = b
            item[3] = "peso"

    # --- 3) que procedencias lleva cada archivo: las que cuadran seguido
    # (un archivo puede llevar dos: Arturo Quezada sale en bascula como
    # MINERAL ARTURO QUEZADA y como SAN LORENZO ING AQ). Un boleto suelto de
    # otra procedencia no cuenta: ese es justo el que hay que avisar.
    fams_arch, prim_arch = {}, {}
    for arch in excels:
        cuenta = {}
        for a, r, b, t in filas_todas:
            if a == arch and b is not None:
                k = _familia(b.get("grupo"))
                cuenta[k] = cuenta.get(k, 0) + 1
        if not cuenta:
            continue
        total = sum(cuenta.values())
        prim_arch[arch] = max(cuenta.items(), key=lambda x: x[1])[0]
        fams_arch[arch] = {k for k, n in cuenta.items() if n >= max(2, total * 0.1)} | {prim_arch[arch]}

    # procedencias que comparten archivo se reportan juntas
    padre = {}
    def raiz_de(x):
        while padre.setdefault(x, x) != x:
            x = padre[x]
        return x
    for fs in fams_arch.values():
        fs = sorted(fs)
        for f in fs:
            raiz_de(f)
        for f in fs[1:]:
            padre[raiz_de(f)] = raiz_de(fs[0])
    miembros = {}
    for f in padre:
        miembros.setdefault(raiz_de(f), set()).add(f)
    nombre_grupo = {}
    for r_, ms in miembros.items():
        for f in ms:
            nombre_grupo[f] = " + ".join(sorted(ms))

    def grupo_de_fam(fam):
        return nombre_grupo.get(fam)

    def grupo_de_fila(a, b):
        if b is not None and _familia(b.get("grupo")) in fams_arch.get(a, ()):
            return nombre_grupo[_familia(b.get("grupo"))]
        return nombre_grupo.get(prim_arch.get(a))

    # hasta donde cubre cada grupo: fecha y hora del ultimo boleto cuadrado
    cubre = {}
    for a, r, b, t in filas_todas:
        g = grupo_de_fila(a, b)
        if not g:
            continue
        c = cubre.setdefault(g, {"desde": r["fecha"], "hasta": r["fecha"], "ult": "", "archivos": set()})
        c["archivos"].add(a)
        c["desde"] = min(c["desde"], r["fecha"])
        c["hasta"] = max(c["hasta"], r["fecha"])
        if b is not None and grupo_de_fam(_familia(b.get("grupo"))) == g:
            c["ult"] = max(c["ult"], b.get("entrada") or "")

    alertas = {}

    def lado_excel(arch, r):
        return {"archivo": arch, "hoja": r["hoja"], "fila": r["fila"], "fecha": r["fecha"],
                "bruto": r["bruto"], "tara": r["tara"], "neto": r["bruto"] - r["tara"],
                "chofer": r.get("chofer"), "camion": r.get("camion"), "lote": r.get("lote"),
                "folio": r.get("folio"), "boleto_excel": r.get("boleto")}

    # V4.7: folio de la tarja de cada boleto. Manda el de bascula (Dato
    # adicional); si no trae, el del Excel donde cuadro.
    tarjas_excel = {}
    for a, r, b, t in filas_todas:
        if b is not None and r.get("folio"):
            tarjas_excel.setdefault(str(b["boleto"]), r["folio"])

    def tarja_de(b):
        if b.get("tarja"):
            return b["tarja"], "báscula"
        f = tarjas_excel.get(str(b["boleto"]))
        return (f, "Excel") if f else (None, None)

    def lado_bascula(b):
        return {"boleto": b["boleto"], "fecha": b["_f"], "entrada": b.get("entrada"),
                "bruto": b.get("bruto"), "tara": b.get("tara"), "neto": b.get("neto"),
                "chofer": b.get("chofer"), "placas": b.get("placas"),
                "procedencia": b.get("procedencia"), "grupo": b.get("grupo"),
                "tarja": tarja_de(b)[0], "pesada": b.get("pesada")}

    for a, r, b, t in filas_todas:
        if a not in fams_arch:
            continue          # archivo que no cuadra con nada: no es de bascula
        fam = grupo_de_fila(a, b) or ""
        if r.get("neto") is not None and abs(r["neto"] - (r["bruto"] - r["tara"])) > 1:
            k = "n-" + _llave(a, r["hoja"], r["fecha"], r["bruto"], r["tara"])
            alertas[k] = {"tipo": "neto", "familia": fam, "fecha": r["fecha"],
                          "excel": dict(lado_excel(a, r), neto_excel=r["neto"]),
                          "bascula": lado_bascula(b) if b else None,
                          "pista": "En el Excel el neto dice %s y bruto - tara da %s. Revisar la celda %s%s."
                                   % ("{:,}".format(r["neto"]), "{:,}".format(r["bruto"] - r["tara"]),
                                      r.get("c_neto") or "", r["fila"])}
        if b is None:
            k = "x-" + _llave(a, r["hoja"], r["fecha"], r["bruto"], r["tara"])
            pista = "No hay boleto con ese bruto y tara entre el %s y el %s." % (
                _fecha_corta(dia(r["fecha"]) - timedelta(days=CUADRE_DIAS)),
                _fecha_corta(dia(r["fecha"]) + timedelta(days=CUADRE_DIAS)))
            nb = r.get("boleto")
            if nb and nb in excluidos:
                pista = ("El Excel trae el boleto %s, que está EXCLUIDO del Flash (%s). "
                         "Si de verdad fue un viaje, restáurelo; si no, quite la fila del Excel."
                         % (nb, excluidos[nb].get("motivo") or "sin motivo"))
            elif nb and nb in por_num:
                o = por_num[nb]
                pista += " El Excel dice boleto %s, pero ese trae bruto %s y tara %s." % (
                    nb, "{:,}".format(int(o.get("bruto") or 0)), "{:,}".format(int(o.get("tara") or 0)))
            alertas[k] = {"tipo": "falta_bascula", "familia": fam, "fecha": r["fecha"],
                          "excel": lado_excel(a, r), "bascula": None, "pista": pista}
            continue
        if r.get("boleto") and r["boleto"] != b["boleto"]:
            alertas["b-%s" % b["boleto"]] = {
                "tipo": "boleto", "familia": fam, "fecha": r["fecha"], "boleto": b["boleto"],
                "dif": ["boleto"], "excel": lado_excel(a, r), "bascula": lado_bascula(b),
                "pista": "En el Excel dice boleto %s, pero bruto y tara son los del boleto %s. "
                         "Revisar el número de boleto en el Excel." % (r["boleto"], b["boleto"])}
        tb = b.get("tarja")
        if tb and r.get("folio") and _tarja_num(tb) != _tarja_num(r["folio"]):
            alertas["f-%s" % b["boleto"]] = {
                "tipo": "folio", "familia": fam, "fecha": r["fecha"], "boleto": b["boleto"],
                "dif": ["folio"], "excel": lado_excel(a, r), "bascula": lado_bascula(b),
                "pista": "La tarja en báscula dice %s y en el Excel dice %s (celda %s%s). "
                         "Revisar cuál es el bueno con la tarja física." % (tb, r["folio"], r.get("c_folio") or "", r["fila"])}
        if t == "peso":
            campos, celda = [], []
            if int(b["bruto"]) != r["bruto"]:
                campos.append("bruto"); celda.append("%s%s" % (r["c_bruto"], r["fila"]))
            if int(b.get("tara") or 0) != r["tara"]:
                campos.append("tara"); celda.append("%s%s" % (r["c_tara"], r["fila"]))
            alertas["p-%s" % b["boleto"]] = {
                "tipo": "peso", "familia": fam, "fecha": r["fecha"], "boleto": b["boleto"],
                "dif": campos, "excel": lado_excel(a, r), "bascula": lado_bascula(b),
                "pista": "Difiere %s en %s kg. Revisar la celda %s del Excel o el boleto."
                         % (" y ".join(campos),
                            "{:,}".format(abs(int(b["bruto"]) - r["bruto"]) + abs(int(b.get("tara") or 0) - r["tara"])),
                            ", ".join(celda))}
        if a in fams_arch and _familia(b.get("grupo")) not in fams_arch[a]:
            alertas["d-%s" % b["boleto"]] = {
                "tipo": "procedencia", "familia": fam, "fecha": r["fecha"], "boleto": b["boleto"],
                "dif": ["procedencia"], "excel": lado_excel(a, r), "bascula": lado_bascula(b),
                "pista": "El Excel %s lo trae, pero báscula lo registró como %s."
                         % (re.sub(r"\.xlsx$", "", a.split("/")[-1], flags=re.I), b.get("procedencia") or b.get("grupo") or "?")}

    # --- 4) boletos de bascula sin fila, dentro de lo que el Excel ya cubre
    for b in boletos:
        if b["boleto"] in usados or not b["_f"]:
            continue
        fam = grupo_de_fam(_familia(b.get("grupo")))
        c = cubre.get(fam)
        if not c or b["_f"] < c["desde"] or b["_f"] > c["hasta"]:
            continue
        if b["_f"] == c["hasta"] and (b.get("entrada") or "") > c["ult"]:
            continue          # el Excel aun no llega a ese camion
        if _es_incompleto(b):
            continue          # va como incompleto, abajo
        pista = "No aparece en %s." % ", ".join(sorted(re.sub(r"\.xlsx$", "", x.split("/")[-1], flags=re.I) for x in c["archivos"]))
        for o in boletos:
            if o is b or o["_f"] != b["_f"] or not o.get("bruto"):
                continue
            mismo_chofer = (o.get("chofer") or "") == (b.get("chofer") or "") and b.get("chofer")
            if mismo_chofer and abs(int(o["bruto"]) - int(b["bruto"])) <= 20:
                pista = ("Mismo chofer, día y bruto que el boleto %s (ese sí está en el Excel). "
                         "Posible pesada duplicada." % o["boleto"]) if o["boleto"] in usados else pista
                break
        alertas["s-%s" % b["boleto"]] = {"tipo": "falta_excel", "familia": fam, "fecha": b["_f"],
                                         "boleto": b["boleto"], "excel": None,
                                         "bascula": lado_bascula(b), "pista": pista}

    # --- 5) boletos incompletos de las procedencias que llevan Excel
    for b in boletos:
        fam = grupo_de_fam(_familia(b.get("grupo")))
        c = cubre.get(fam)
        if not c or not b["_f"] or b["_f"] < c["desde"] or not _es_incompleto(b):
            continue
        que = "tara 0" if not b.get("tara") else "neto 0"
        alertas["i-%s" % b["boleto"]] = {
            "tipo": "incompleto", "familia": fam, "fecha": b["_f"], "boleto": b["boleto"],
            "dif": ["tara" if que == "tara 0" else "neto"],
            "excel": lado_excel(*usados_fila(usados, b, filas_todas)) if b["boleto"] in usados else None,
            "bascula": lado_bascula(b),
            "pista": "Boleto con %s en báscula.%s" % (que, " Infla el total %s t." % _ton(b.get("neto"))
                                                     if que == "tara 0" and b.get("neto") else "")}

    # --- 5b) posible duplicado EN BASCULA (no depende del Excel)
    # Mismo dia, el MISMO chofer (aunque el nombre traiga un dedazo) y casi
    # el mismo bruto. Las placas solo sirven para descartar: si las dos
    # traen placas de verdad y son distintas, son camiones distintos. "SN",
    # "S/N" y similares no cuentan como placas (las comparten muchos).
    # V4.6: antes bastaban placas iguales, y "SN" junto camiones distintos.
    def _quien(b):
        pl = re.sub(r"[^A-Z0-9]", "", _sin_acentos(b.get("placas")))
        if pl in ("", "SN", "SINPLACAS", "SINPLACA", "NA", "0", "00", "000"):
            pl = ""
        return (_sin_acentos(b.get("chofer")), pl)
    def _mismo_chofer(x, y):
        if not x or not y:
            return False
        if x == y:
            return True
        import difflib
        return difflib.SequenceMatcher(None, x, y).ratio() >= 0.88
    def _min(b):
        try:
            e = datetime.fromisoformat((b.get("entrada") or "")[:19])
            return e.hour * 60 + e.minute
        except Exception:
            return None
    def _sugerir(orig, dup):
        """V4.8: cual excluir y por que, en palabras de bascula."""
        por = []
        if orig["boleto"] in usados and dup["boleto"] not in usados:
            por.append("el %s sí está en el Excel y este no" % orig["boleto"])
        if not dup.get("tara"):
            por.append("trae tara 0")
        elif _es_incompleto(dup):
            por.append("trae neto 0")
        if str(dup.get("pesada") or "").startswith("M") and not str(orig.get("pesada") or "").startswith("M"):
            por.append("su pesada fue manual")
        if not tarja_de(dup)[0] and tarja_de(orig)[0]:
            por.append("no tiene tarja y el %s sí (%s)" % (orig["boleto"], tarja_de(orig)[0]))
        if not por:
            por.append("es el que se pesó después")
        return {"excluir": dup["boleto"], "conservar": orig["boleto"], "por": por}

    por_dia = {}
    for b in boletos:
        if b.get("bruto") and b["_f"]:
            por_dia.setdefault(b["_f"], []).append(b)
    vistos = set()
    for dia_l in por_dia.values():
        dia_l.sort(key=lambda x: (x.get("entrada") or "", x["boleto"]))
        for i, a in enumerate(dia_l):
            for b in dia_l[i + 1:]:
                qa, qb = _quien(a), _quien(b)
                if not _mismo_chofer(qa[0], qb[0]):
                    continue
                if qa[1] and qb[1] and qa[1] != qb[1]:
                    continue          # placas de verdad y distintas: otro camion
                db = abs(int(a["bruto"]) - int(b["bruto"]))
                ma, mb = _min(a), _min(b)
                cerca = ma is not None and mb is not None and abs(mb - ma) <= CUADRE_DUP_MIN
                igual = db == 0 and int(a.get("tara") or 0) == int(b.get("tara") or 0)
                if not ((db <= CUADRE_DUP_KG and cerca) or igual):
                    continue
                # se marca el que NO esta en el Excel; si ninguno o los dos, el ultimo
                if a["boleto"] in usados and b["boleto"] not in usados:
                    orig, dup = a, b
                elif b["boleto"] in usados and a["boleto"] not in usados:
                    orig, dup = b, a
                elif _es_incompleto(a) and not _es_incompleto(b):
                    orig, dup = b, a
                else:
                    orig, dup = a, b
                if dup["boleto"] in vistos:
                    continue
                vistos.add(dup["boleto"])
                fam = grupo_de_fam(_familia(dup.get("grupo"))) or _familia(dup.get("grupo"))
                extra = ""
                if not dup.get("tara"):
                    extra = " Además, este boleto trae tara 0."
                alertas.pop("s-%s" % dup["boleto"], None)
                alertas.pop("i-%s" % dup["boleto"], None)
                alertas["u-%s" % dup["boleto"]] = {
                    "tipo": "duplicado", "familia": fam, "fecha": dup["_f"], "boleto": dup["boleto"],
                    "dif": ["bruto"] if db else [],
                    "excel": lado_excel(*usados_fila(usados, dup, filas_todas)) if dup["boleto"] in usados else None,
                    "excel_otro": lado_excel(*usados_fila(usados, orig, filas_todas)) if orig["boleto"] in usados else None,
                    "sugerencia": _sugerir(orig, dup),
                    "otro": lado_bascula(orig), "bascula": lado_bascula(dup),
                    "pista": ("Mismo %s, mismo día y bruto %s que el boleto %s%s. Posible pesada "
                              "duplicada: si es así, exclúyala del Flash (Alertas → Boletos excluidos).%s"
                              % ("chofer" + (" y camión" if qa[1] and qa[1] == qb[1] else ""),
                                 "igual" if db == 0 else "casi igual (%d kg)" % db, orig["boleto"],
                                 " (ese sí está en el Excel)" if orig["boleto"] in usados else "",
                                 extra))}

    # --- 5c) V4.7: posible tarja usada dos veces
    # Los folios se repiten (blocks del 0001 al 5000), asi que el folio solo
    # no basta: misma procedencia, a menos de CUADRE_TARJA_DIAS dias y
    # ademas el mismo chofer, las mismas placas (de verdad) o casi el mismo
    # bruto. Si ya salio como duplicado, no se repite el aviso.
    por_tarja = {}
    for b in boletos:
        n = _tarja_num(tarja_de(b)[0])
        if not n or not b["_f"]:
            continue
        fam_b = grupo_de_fam(_familia(b.get("grupo"))) or _familia(b.get("grupo"))
        por_tarja.setdefault((fam_b, n), []).append(b)
    for (fam_b, n), lista in por_tarja.items():
        if len(lista) < 2:
            continue
        lista.sort(key=lambda x: (x.get("entrada") or x["_f"], x["boleto"]))
        for i, a in enumerate(lista):
            for b in lista[i + 1:]:
                if abs((dia(b["_f"]) - dia(a["_f"])).days) > CUADRE_TARJA_DIAS:
                    continue
                if "u-%s" % a["boleto"] in alertas or "u-%s" % b["boleto"] in alertas:
                    continue
                qa, qb = _quien(a), _quien(b)
                amarre = []
                if _mismo_chofer(qa[0], qb[0]):
                    amarre.append("chofer")
                if qa[1] and qa[1] == qb[1]:
                    amarre.append("placas")
                if a.get("bruto") and b.get("bruto") and abs(int(a["bruto"]) - int(b["bruto"])) <= CUADRE_DUP_KG:
                    amarre.append("bruto")
                if not amarre:
                    continue          # mismo folio de otro block: normal
                k = "t-%s" % b["boleto"]
                if k in alertas:
                    continue
                ta, oa = tarja_de(a)
                tb, ob = tarja_de(b)
                alertas[k] = {
                    "tipo": "tarja", "familia": fam_b, "fecha": b["_f"], "boleto": b["boleto"],
                    "dif": ["tarja"], "excel": None,
                    "otro": lado_bascula(a), "bascula": lado_bascula(b),
                    "pista": "La tarja %s viene en el boleto %s (%s, según %s) y en el %s (%s, según %s), con el mismo %s. "
                             "Una tarja es un solo viaje: revisar la tarja física."
                             % ("%04d" % n, a["boleto"], _fecha_corta(dia(a["_f"])), oa,
                                b["boleto"], _fecha_corta(dia(b["_f"])), ob,
                                (", ".join(amarre[:-1]) + " y " + amarre[-1]) if len(amarre) > 1 else amarre[0])}

    # --- 6) resumen por procedencia
    por_fam = {}
    for a, r, b, t in filas_todas:
        fam = grupo_de_fila(a, b)
        if not fam:
            continue
        p = por_fam.setdefault(fam, {"familia": fam, "archivos": set(), "viajes_excel": 0, "cuadran": 0,
                                     "kg_excel": 0, "kg_bascula": 0, "filas": [], "sin_fila": [],
                                     "alertas": 0})
        p["archivos"].add(a)
        p["viajes_excel"] += 1
        p["kg_excel"] += r["bruto"] - r["tara"]
        if b is not None and t == "ok" and grupo_de_fam(_familia(b.get("grupo"))) == fam:
            p["cuadran"] += 1
    for b in boletos:
        fam = grupo_de_fam(_familia(b.get("grupo")))
        c = cubre.get(fam)
        if fam in por_fam and c and c["desde"] <= b["_f"] <= c["hasta"] and \
                not (b["_f"] == c["hasta"] and (b.get("entrada") or "") > c["ult"]):
            por_fam[fam]["kg_bascula"] += int(b.get("neto") or 0)
    for k, al in alertas.items():
        p = por_fam.get(al.get("familia"))
        if not p:
            continue
        p["alertas"] += 1
        if al.get("excel"):
            p["filas"].append("%s·%s" % (al["excel"]["archivo"], al["excel"]["fila"]))
        elif al.get("boleto"):
            p["sin_fila"].append(al["boleto"])

    resumen = {}
    for fam, p in por_fam.items():
        c = cubre.get(fam) or {}
        resumen[clave_fb(fam)] = {
            "familia": fam, "archivos": sorted(p["archivos"]), "viajes_excel": p["viajes_excel"],
            "cuadran": p["cuadran"], "t_excel": _ton(p["kg_excel"]), "t_bascula": _ton(p["kg_bascula"]),
            "diferencia_t": _ton(p["kg_bascula"] - p["kg_excel"]),
            "desde": c.get("desde"), "hasta": c.get("hasta"),
            "filas": sorted(set(p["filas"]))[:60], "sin_fila": sorted(set(p["sin_fila"]))[:60],
            "alertas": p["alertas"],
        }
    # Tope de seguridad: si algo sale muy raro (un Excel nuevo con otro
    # formato, Firebase a medias) no se manda una avalancha. Se quedan las
    # mas recientes y se avisa.
    recortadas = 0
    if len(alertas) > CUADRE_MAX_ALERTAS:
        orden = sorted(alertas, key=lambda k: alertas[k].get("fecha") or "", reverse=True)
        recortadas = len(alertas) - CUADRE_MAX_ALERTAS
        alertas = {k: alertas[k] for k in orden[:CUADRE_MAX_ALERTAS]}
    viajes = sum(p["viajes_excel"] for p in por_fam.values())
    cuadran = sum(p["cuadran"] for p in por_fam.values())
    return {"lista": alertas, "por_procedencia": resumen,
            "viajes_excel": viajes, "cuadran": cuadran,
            "archivos_leidos": len(excels), "archivos_con_datos": len(fams_arch),
            "recortadas": recortadas,
            "sin_cuadre": sorted(a for a in excels if a not in fams_arch),
            "tarjas": tarjas_excel}


def usados_fila(usados, b, filas_todas):
    arch, fila = usados[b["boleto"]]
    for a, r, bb, t in filas_todas:
        if a == arch and r["fila"] == fila:
            return a, r
    return arch, {"hoja": "", "fila": fila, "fecha": b["_f"], "bruto": 0, "tara": 0}


def _fecha_corta(d):
    meses = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
    return "%d-%s-%02d" % (d.day, meses[d.month - 1], d.year % 100)


def _cuadre_cache_leer():
    try:
        with open(ARCHIVO_CUADRE_CACHE, "r", encoding="utf-8") as f:
            c = json.load(f)
        return c if isinstance(c, dict) else {}
    except Exception:
        return {}


def _cuadre_cache_guardar(c):
    try:
        tmp = ARCHIVO_CUADRE_CACHE + ".nuevo"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(c, f, ensure_ascii=False)
        os.replace(tmp, ARCHIVO_CUADRE_CACHE)
    except Exception as e:
        log("Alertas: no se pudo guardar la memoria local (%s)." % e, "WARN")


def revisar_cuadre(hoy):
    """Corre el cuadre si toca. Devuelve un texto corto para /salud."""
    if not CFG.get("cuadre", True):
        return "apagado"
    t0 = time.time()
    hoy_txt = hoy.strftime("%Y-%m-%d")
    desde = str(CFG.get("cuadre_desde") or "2026-09-01")[:10]
    cache = _cuadre_cache_leer()

    carpetas, error = _carpetas_excel(desde, hoy_txt)
    if error:
        log("Alertas: %s." % error, "WARN")
        enviar("alertas/estado", {"ok": False, "error": error,
                                  "revisado": datetime.now().isoformat(timespec="seconds")})
        return error
    raiz, lista_carpetas = carpetas

    # Que archivos hay y como estan. Solo se re-lee el que cambio.
    archivos = {}
    for carpeta in lista_carpetas:
        for dirpath, _, nombres in os.walk(carpeta):
            for n in nombres:
                if not n.lower().endswith(".xlsx") or n.startswith("~$"):
                    continue
                ruta = os.path.join(dirpath, n)
                try:
                    st = os.stat(ruta)
                except Exception:
                    continue
                rel = os.path.relpath(ruta, raiz).replace("\\", "/")
                archivos[rel] = (ruta, "%d-%d" % (int(st.st_mtime), st.st_size))
    firma = hashlib.sha1(json.dumps(sorted((k, v[1]) for k, v in archivos.items())).encode()).hexdigest()

    cada = max(3, int(CFG.get("cuadre_cada_min", 15)))
    ultimo = cache.get("ultimo") or ""
    # V4.7: si alguien excluye o restaura un boleto, se cuadra de inmediato
    firma_excl = hashlib.sha1(json.dumps(sorted(EXCL)).encode()).hexdigest()
    if cache.get("firma") == firma and cache.get("desde") == desde and ultimo and \
            cache.get("excl") == firma_excl and cache.get("version") == VERSION and \
            (datetime.now() - datetime.fromisoformat(ultimo)).total_seconds() < cada * 60:
        return "sin cambios"

    previo = cache.get("archivos") or {}
    nuevos = {}
    errores = []
    for rel, (ruta, huella) in archivos.items():
        p = previo.get(rel)
        if p and p.get("huella") == huella and p.get("desde") == desde and p.get("v") == VERSION:
            nuevos[rel] = p
            continue
        try:
            filas = leer_excel_fletes(ruta, desde, hoy_txt)
            nuevos[rel] = {"huella": huella, "desde": desde, "filas": filas, "v": VERSION}
        except Exception as e:
            errores.append("%s (%s)" % (rel, e))
            if p:
                nuevos[rel] = p          # se queda con lo ultimo que si pudo leer
    excels = {}
    for rel, p in nuevos.items():
        if p.get("filas"):
            nombre = rel.split("/", 1)[-1]
            excels[nombre] = p["filas"]

    # Boletos ya cerrados en Firebase. Para no bajar semanas de boletos cada
    # 15 minutos, se guardan en la memoria local: el rango completo se baja
    # una vez al dia y en las demas vueltas solo los ultimos 3 dias.
    ini = (datetime.strptime(desde, "%Y-%m-%d") - timedelta(days=CUADRE_DIAS)).strftime("%Y-%m-%d")
    dias_b = cache.get("bol_dias") or {}
    completo = cache.get("bol_full") != hoy_txt or cache.get("bol_ini") != ini
    pedir_desde = ini if completo else (hoy - timedelta(days=3)).strftime("%Y-%m-%d")
    datos = firebase_get_rango("boletos", pedir_desde, hoy_txt)
    if datos is None:
        return "sin red"
    if completo:
        dias_b = {}
    for f in [f for f in dias_b if f >= pedir_desde or f < ini]:
        del dias_b[f]
    campos = ("boleto", "entrada", "salida", "bruto", "tara", "neto", "grupo",
              "procedencia", "chofer", "placas", "tarja", "pesada")
    for fecha_k, dia_b in datos.items():
        if not isinstance(dia_b, dict):
            continue
        guardar = {}
        for k, b in dia_b.items():
            if not isinstance(b, dict) or b.get("abierto") or b.get("origen_dato") == "excel":
                continue
            if not str(k).isdigit():
                continue
            guardar[str(k)] = {c: b.get(c) for c in campos if b.get(c) is not None}
        dias_b[fecha_k] = guardar
    boletos = []
    for fecha_k, dia_b in dias_b.items():
        for b in dia_b.values():
            b = dict(b)
            b["_fecha"] = fecha_k
            boletos.append(b)
    if completo:
        cache["bol_full"] = hoy_txt
        cache["bol_ini"] = ini
    cache["bol_dias"] = dias_b

    # V4.7: los excluidos no se cuadran (se lee la lista de esta corrida, no
    # lo que trae la memoria: asi una exclusion de un dia viejo cuenta ya).
    boletos = [b for b in boletos if _entero(b.get("boleto")) not in EXCL]
    rep = cuadrar(excels, boletos, hoy_txt, EXCL)
    tarjas = rep.pop("tarjas", {}) or {}
    firma_t = hashlib.sha1(json.dumps(tarjas, sort_keys=True).encode()).hexdigest()
    if cache.get("tarjas") != firma_t:
        enviar("tarjas_excel", tarjas)       # {boleto: "0889"} solo cuando cambia
        cache["tarjas"] = firma_t
    rep["estado"] = {
        "ok": True, "revisado": datetime.now().isoformat(timespec="seconds"),
        "desde": desde, "carpeta": raiz, "archivos_vistos": len(archivos),
        "archivos_con_datos": rep.pop("archivos_con_datos"), "archivos_leidos": rep.pop("archivos_leidos"),
        "recortadas": rep.pop("recortadas") or None, "sin_cuadre": rep.pop("sin_cuadre") or None,
        "errores": errores[:10] or None, "segundos": round(time.time() - t0, 1),
        "agente": VERSION,
    }
    enviar("alertas", rep)
    cache.update({"firma": firma, "desde": desde, "ultimo": datetime.now().isoformat(timespec="seconds"),
                  "archivos": nuevos, "excl": firma_excl, "version": VERSION})
    _cuadre_cache_guardar(cache)
    txt = "%d alertas, %d de %d viajes cuadran" % (len(rep["lista"]), rep["cuadran"], rep["viajes_excel"])
    log("Alertas: %s (%.1fs)." % (txt, time.time() - t0))
    return txt


def rango_dia(fecha):
    d = fecha.strftime("%Y-%m-%d")
    return "%s 00:00" % d, "%s 23:59" % d


def main():
    t0 = time.time()

    if not INSTALADO[0]:
        aviso = ("Esta es una copia suelta del agente: no hay config.json en su "
                 "carpeta.\nNo hace nada y no toca ningun archivo.\n\n"
                 "El agente instalado vive en C:\\FlashAcarreo y corre solo "
                 "cada 3 minutos.")
        try:
            print(aviso)
        except Exception:
            pass
        return 0

    if os.path.exists(ARCHIVO_STOP):
        log("Archivo STOP presente. El agente no hace nada.")
        return 0

    orden = leer_orden()

    # Los candados se revisan DESPUES de leer la orden, a proposito. Si se
    # revisaran antes, un dato mal escrito -- el nombre del equipo con un
    # dedazo -- dejaria al agente muerto y sin forma de corregirlo a
    # distancia: habria que ir a bascula. Leyendo primero la orden, siempre
    # queda la puerta abierta para desactivarlos desde GitHub.
    motivo = revisar_candados()
    if motivo:
        log(motivo)
        try:
            print(motivo)
        except Exception:
            pass
        return 0      # sin tocar Firebase, sin tocar Dropbox, sin respaldos

    # Se revisa antes que nada: asi una version nueva puede llegar incluso
    # con el agente apagado, que es justo cuando mas falta hace.
    version_nueva = autoactualizar(orden)

    # V4.2: "solo_apagar_version" permite apagar UNA version vieja sin apagar
    # la nueva. Sirve para que la corrida que todavia es la vieja solo se
    # actualice y se detenga, y la siguiente (ya nueva) haga el trabajo.
    solo = CFG.get("solo_apagar_version")
    if CFG.get("apagado") and solo and str(solo) != VERSION:
        log("Orden de apagado solo para la version %s; esta es %s y sigue." % (solo, VERSION))
        CFG["apagado"] = False

    if CFG.get("apagado"):
        log("Apagado por orden de Dropbox. El agente no hace nada.")
        escribir_estado({"agente": VERSION, "modo": "apagado",
                         "version_pendiente": version_nueva,
                         "ultima_corrida": datetime.now().isoformat(timespec="seconds")})
        return 0

    log("--- Flash Acarreo V%s | %s ---"
        % (VERSION, "MODO OBSERVACION (no envia)" if CFG.get("modo_observacion")
           else "PRODUCCION"))

    if not CFG.get("modo_observacion"):
        vaciar_cola()

    hoy = datetime.now()

    # Una carga historica pedida desde Dropbox se hace UNA sola vez por 'id',
    # para que no se repita en cada corrida.
    n_dias = max(1, int(CFG.get("dias_hacia_atras", 1)))
    carga = None
    try:
        pedido = int(orden.get("carga_dias") or 0)
    except Exception:
        pedido = 0
    # V4.2: "carga_desde" (aaaa-mm-dd) gana sobre "carga_dias": el agente
    # calcula solo cuantos dias faltan para llegar a esa fecha, el dia que
    # corra. Asi no importa si la orden se sube hoy o dentro de una semana.
    desde_txt = str(orden.get("carga_desde") or "").strip()
    if desde_txt:
        try:
            ini = datetime.strptime(desde_txt[:10], "%Y-%m-%d")
            pedido = max(1, (hoy.date() - ini.date()).days)
            log("Carga desde %s: %d dias hacia atras." % (desde_txt[:10], pedido))
        except Exception:
            log("carga_desde no tiene forma de fecha (%s). Se ignora." % desde_txt, "WARN")
    if pedido > 0:
        ident = str(orden.get("id") or ("carga-" + str(pedido)))
        if orden_ya_hecha(ident):
            log("La carga '%s' ya se habia hecho. Corrida normal." % ident)
        else:
            carga = ident
            n_dias = pedido
            log("CARGA HISTORICA pedida desde Dropbox: %d dias (orden '%s')" % (pedido, ident))

    # ---- campanas ----
    # Se leen UNA vez por corrida. Si no hay internet o la rama no existe,
    # n_campanas queda en 0 y todo sigue igual que en la V3.9.
    # Cinturon y tirantes: firebase_get ya atrapa sus errores, pero esto es
    # la PC de bascula. Ninguna novedad de las campanas puede tumbar el
    # envio de boletos, que es para lo que existe el agente.
    try:
        n_campanas = cargar_campanas()
    except Exception as e:
        log("Campanas: no se pudieron cargar (%s). Corrida sin lotes." % e, "WARN")
        CAMPANAS.clear()
        n_campanas = 0
    if n_campanas:
        log("Campanas: %d lote(s) en %d procedencia(s)" % (n_campanas, len(CAMPANAS)))

    # Cuando alguien mueve el arranque de un lote hacia atras, los dias viejos
    # ya tienen su resumen escrito sin ese lote. El panel lo pide escribiendo
    # /campanas/_recalcular y aqui se atiende UNA sola vez por 'id'.
    recalculo = None
    if CFG.get("campanas", True):
        try:
            pedido_rec = firebase_get("campanas/_recalcular")
        except Exception as e:
            log("No se pudo leer el recalculo: %s" % e, "WARN")
            pedido_rec = None
        if isinstance(pedido_rec, dict):
            ident = "recalc-" + str(pedido_rec.get("id") or "")
            try:
                dias_rec = int(pedido_rec.get("dias") or 0)
            except Exception:
                dias_rec = 0
            if dias_rec > 0 and not orden_ya_hecha(ident):
                recalculo = ident
                n_dias = max(n_dias, min(dias_rec, 400))
                log("RECALCULO pedido desde el panel: %d dias (%s)" % (dias_rec, ident))

    # ---- V4.7: boletos excluidos del Flash ----
    # Se leen cada corrida (es una lista corta). Si alguien excluyo o
    # restauro un boleto, su dia se vuelve a procesar en esta misma corrida.
    excl_cambios = None
    try:
        excl_cambios = cargar_excluidos()
    except Exception as e:
        log("Excluidos: no se pudieron revisar (%s). Corrida normal." % e, "WARN")
    if EXCL:
        log("Excluidos del Flash: %d boleto(s)" % len(EXCL))

    dias = [hoy - timedelta(days=i) for i in range(0, n_dias + 1)]

    # V4.3: viajes de Excel. En la corrida de la carga se agregan sus dias.
    carga_xls = preparar_excel(orden)
    if carga_xls:
        ya = {d.strftime("%Y-%m-%d") for d in dias}
        for f in EXCEL:
            if f not in ya:
                dias.append(datetime.strptime(f, "%Y-%m-%d"))
        dias.sort(reverse=True)
        log("CARGA EXCEL '%s': %d dias a procesar." % (carga_xls, len(dias)))
    xls_falla = False

    # Los dias de los excluidos se agregan DESPUES de armar la ventana normal:
    # las campanas solo ven la ventana normal (seguida), para que un dia
    # suelto de hace un mes no confunda el calculo del tope.
    normales = {d.strftime("%Y-%m-%d") for d in dias}
    inicio_normal = dias[-1].strftime("%Y-%m-%d")
    excl_dias = sorted(f for f in (excl_cambios or {}) if f)
    for f in excl_dias:
        if f not in normales and f not in {d.strftime("%Y-%m-%d") for d in dias}:
            dias.append(datetime.strptime(f, "%Y-%m-%d"))
    if excl_dias:
        dias.sort(reverse=True)
        log("Excluidos: cambio en %s. Se recalcula: %s." % (
            ", ".join(str(b) for v in excl_cambios.values() for b in v), ", ".join(excl_dias)))
    excl_falla = False

    total_cerrados = 0
    resumen_dia = {}

    # ---- boletos cerrados ----
    # V4.1: primero se leen TODOS los dias, para que el cierre por tope vea
    # los boletos en orden antes de ponerles su lote.
    leidos = []
    for d in dias:
        desde, hasta = rango_dia(d)
        crudos = leer_webservice(CFG["ws_cerrados"], desde, hasta)
        if crudos is None:
            if d.strftime("%Y-%m-%d") in EXCEL:
                xls_falla = True     # sin bascula no se mezcla: se reintenta
            if d.strftime("%Y-%m-%d") in excl_dias:
                excl_falla = True    # se recalcula en la siguiente corrida
            continue
        registros = [r for r in (normalizar(c, abierto=False) for c in crudos) if r]
        leidos.append((d, d.strftime("%Y-%m-%d"), registros))

    campanas_abiertas = []
    if n_campanas:
        try:
            campanas_abiertas = avanzar_campanas(
                [r for _, f, regs in leidos if f in normales for r in regs], inicio_normal)
            for h in campanas_abiertas:
                log("Campanas: " + h)
        except Exception as e:
            log("Campanas: no se pudo revisar el tope (%s). Corrida normal." % e, "WARN")

    for idx, (d, fecha_k, registros) in enumerate(leidos):
        for r in registros:
            # El lote se calcula, no se captura. Si esa procedencia no
            # lleva campanas queda en None y Firebase borra el campo,
            # asi que un cambio de campana se limpia solo al reprocesar.
            try:
                r["lote"] = lote_de(r.get("grupo"), r.get("entrada"), r.get("boleto"))
            except Exception:
                r["lote"] = None
            r["excluido"] = excluido(r["boleto"])     # V4.7: None borra la marca
            enviar("boletos/%s/%d" % (fecha_k, r["boleto"]), r)
        total_cerrados += len(registros)

        # V4.3: los de Excel de ese dia van con su llave XLS y sin lote,
        # y entran al resumen junto con los de bascula.
        xls = EXCEL.get(fecha_k) or []
        for r in xls:
            enviar("boletos/%s/%s" % (fecha_k, r["boleto"]), r)
        if xls:
            registros = registros + xls
            total_cerrados += len(xls)

        # durante una carga larga, avisa por Dropbox como va
        if (carga or carga_xls) and (idx % 10 == 0 or idx == len(leidos) - 1):
            vaciar_lote()
            escribir_estado({
                "agente": VERSION, "equipo": os.environ.get("COMPUTERNAME", "?"),
                "modo": "carga historica en curso", "orden": carga or carga_xls,
                "avance": "%d de %d dias" % (idx + 1, len(leidos)),
                "pct": round((idx + 1) / max(1, len(leidos)) * 100),
                "boletos_hasta_ahora": total_cerrados,
                "peticiones": ENVIADOS[1],
                "ultima_corrida": datetime.now().isoformat(timespec="seconds"),
            })

        if registros:
            # V4.7: los excluidos no cuentan en el resumen (Hoy, Historico,
            # campanas). Si el dia queda vacio, el resumen queda en ceros.
            validos = [r for r in registros if not r.get("excluido")]
            resumen = armar_resumen(fecha_k, validos)
            if len(validos) != len(registros):
                resumen["excluidos"] = {"viajes": len(registros) - len(validos),
                                        "kg": sum(r.get("neto") or 0 for r in registros if r.get("excluido"))}
            enviar("resumen/%s" % fecha_k, resumen)
            if fecha_k == hoy.strftime("%Y-%m-%d"):
                resumen_dia = resumen
            log("%s: %d viajes, %.3f t" % (fecha_k, resumen["viajes"], resumen["toneladas"]))

    # ---- camiones en patio (boletos abiertos) ----
    desde, hasta = rango_dia(hoy)
    patio_crudo = leer_webservice(CFG["ws_patio"], desde, hasta)
    patio = []
    if patio_crudo is not None:
        ahora = datetime.now()
        for c in patio_crudo:
            r = normalizar(c, abierto=True)
            if not r:
                continue
            if r.get("entrada"):
                try:
                    espera = int((ahora - datetime.fromisoformat(r["entrada"]))
                                 .total_seconds() // 60)
                    r["minutos_en_patio"] = espera if 0 <= espera < 60 * 48 else None
                except Exception:
                    pass
            try:
                r["lote"] = lote_de(r.get("grupo"), r.get("entrada"), r.get("boleto"))
            except Exception:
                r["lote"] = None
            patio.append(r)
        patio.sort(key=lambda x: x.get("entrada") or "")
        # el patio es una foto del momento: se reemplaza completo, no se acumula
        enviar("patio", {
            "camiones": patio,
            "total": len(patio),
            "actualizado": datetime.now().isoformat(timespec="seconds"),
        })
        log("Patio: %d camion(es) abiertos" % len(patio))
    else:
        log("Flash_Patio no respondio o no esta configurado todavia.", "WARN")

    if excl_cambios is not None and not CFG.get("modo_observacion"):
        if excl_falla or RED_CAIDA[0]:
            if excl_dias:
                log("Excluidos: no se pudo recalcular (bascula o red). Se repite.", "WARN")
        elif excl_cambios or not os.path.exists(ARCHIVO_EXCL):
            vaciar_lote()
            if not RED_CAIDA[0]:
                guardar_excluidos_vistos()

    if recalculo:
        marcar_orden(recalculo)
        log("RECALCULO terminado sobre %d dias." % len(dias))

    if carga_xls:
        if xls_falla or RED_CAIDA[0]:
            log("CARGA EXCEL incompleta (bascula o red fallaron). Se repite en la siguiente corrida.", "WARN")
            carga_xls = None
        else:
            marcar_orden(carga_xls)
            log("CARGA EXCEL terminada: %d viajes." % sum(len(v) for v in EXCEL.values()))

    if carga:
        marcar_orden(carga)
        log("CARGA HISTORICA terminada: %d dias, %d boletos." % (len(dias), total_cerrados))

    # ---- V4.4: alertas (cuadre contra los Excel de fletes) ----
    # Va despues de mandar los boletos, para comparar contra lo mas nuevo.
    # Nunca en una carga larga ni en modo observacion. Envuelto: un Excel
    # raro no puede tumbar el latido ni el envio de boletos.
    alertas_txt = None
    if not CFG.get("modo_observacion") and not (carga or carga_xls or recalculo):
        try:
            vaciar_lote()
            if not RED_CAIDA[0]:
                alertas_txt = revisar_cuadre(hoy)
        except Exception as e:
            alertas_txt = "error: %s" % e
            log("Alertas: no se pudo hacer el cuadre (%s). Sigue todo igual." % e, "WARN")

    # ---- latido de salud: es lo que permite el sello de frescura del visor ----
    # Se vacia el lote ANTES de armar el latido, para que el conteo que
    # reporta sea el real y no el de antes del ultimo envio.
    vaciar_lote()
    salud = {
        "agente": VERSION,
        "equipo": os.environ.get("COMPUTERNAME", "?"),
        "ultima_corrida": datetime.now().isoformat(timespec="seconds"),
        "boletos_procesados": total_cerrados,
        "camiones_en_patio": len(patio),
        "modo": "observacion" if CFG.get("modo_observacion") else "produccion",
        "cola_pendiente": (sum(1 for _ in open(ARCHIVO_COLA, encoding="utf-8"))
                           if os.path.exists(ARCHIVO_COLA) else 0),
        "duracion_seg": round(time.time() - t0, 2),
        "ws_ok": total_cerrados > 0 or patio_crudo is not None,
        "registros_enviados": ENVIADOS[0],
        "peticiones_firebase": ENVIADOS[1],
        "dias_revisados": len(dias),
        "carga_historica": carga or None,
        "carga_excel": carga_xls or None,
        "excel_viajes": sum(len(v) for v in EXCEL.values()) or None,
        "lotes_cargados": n_campanas,
        "campanas_abiertas": campanas_abiertas or None,
        "recalculo": recalculo or None,
        "version_pendiente": version_nueva,
        "alertas": alertas_txt,
        "excluidos": len(EXCL) or None,
        "carpeta": BASE,
    }
    vaciar_lote()
    enviar("salud", salud)
    vaciar_lote()
    escribir_estado(dict(salud, resumen_hoy=resumen_dia,
                         patio_resumen=[{"boleto": c["boleto"],
                                         "placas": c.get("placas"),
                                         "procedencia": c.get("procedencia"),
                                         "minutos": c.get("minutos_en_patio")}
                                        for c in patio]))

    if CFG.get("modo_observacion"):
        muestra = {"resumen_hoy": resumen_dia, "patio": patio[:3]}
        log("OBSERVACION - nada se envio. Muestra: %s"
            % json.dumps(muestra, ensure_ascii=False)[:1500])

    log("Fin. %d boletos, %d en patio, %.2fs%s"
        % (total_cerrados, len(patio), time.time() - t0,
           ("  ·  %d registros en %d peticion(es)" % (ENVIADOS[0], ENVIADOS[1]))
           if ENVIADOS[1] else ""))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        # Pase lo que pase, este proceso muere en silencio y sin molestar a nadie.
        try:
            import traceback
            log("ERROR NO CONTROLADO: %s\n%s" % (e, traceback.format_exc()), "ERROR")
        except Exception:
            pass
        sys.exit(0)
