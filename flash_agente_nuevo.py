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

VERSION = "4.1"
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


def cargar_campanas():
    """Trae /campanas y la deja lista para consultar. Devuelve cuantos lotes
    quedaron cargados. Cualquier cosa rara se ignora en silencio: mas vale
    un boleto sin lote que una corrida detenida.

    Cada lote arranca por boleto (desde_boleto, V4.1) o por momento (desde,
    V4.0). Puede traer tope_kg y siguiente_boleto."""
    global CAMPANAS
    CAMPANAS = {}
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
        lotes = cuerpo.get("lotes")
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
    filas = CAMPANAS.get(clave_fb(grupo))
    if not filas:
        return None
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
            total += ((((dia or {}).get("por_lote") or {}).get(grupo_k) or {})
                      .get(str(numero)) or {}).get("kg") or 0
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

    dias = [hoy - timedelta(days=i) for i in range(0, n_dias + 1)]

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
            continue
        registros = [r for r in (normalizar(c, abierto=False) for c in crudos) if r]
        leidos.append((d, d.strftime("%Y-%m-%d"), registros))

    campanas_abiertas = []
    if n_campanas:
        try:
            campanas_abiertas = avanzar_campanas(
                [r for _, _, regs in leidos for r in regs], dias[-1].strftime("%Y-%m-%d"))
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
            enviar("boletos/%s/%d" % (fecha_k, r["boleto"]), r)
        total_cerrados += len(registros)

        # durante una carga larga, avisa por Dropbox como va
        if carga and (idx % 10 == 0 or idx == len(leidos) - 1):
            vaciar_lote()
            escribir_estado({
                "agente": VERSION, "equipo": os.environ.get("COMPUTERNAME", "?"),
                "modo": "carga historica en curso", "orden": carga,
                "avance": "%d de %d dias" % (idx + 1, len(leidos)),
                "pct": round((idx + 1) / max(1, len(leidos)) * 100),
                "boletos_hasta_ahora": total_cerrados,
                "peticiones": ENVIADOS[1],
                "ultima_corrida": datetime.now().isoformat(timespec="seconds"),
            })

        if registros:
            resumen = armar_resumen(fecha_k, registros)
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

    if recalculo:
        marcar_orden(recalculo)
        log("RECALCULO terminado sobre %d dias." % len(dias))

    if carga:
        marcar_orden(carga)
        log("CARGA HISTORICA terminada: %d dias, %d boletos." % (len(dias), total_cerrados))

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
        "lotes_cargados": n_campanas,
        "campanas_abiertas": campanas_abiertas or None,
        "recalculo": recalculo or None,
        "version_pendiente": version_nueva,
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
