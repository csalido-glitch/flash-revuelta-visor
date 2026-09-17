# -*- coding: utf-8 -*-
"""
FLASH DE ACARREO - Agente V3
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

VERSION = "3.7"
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
    "timeout_orden": 8
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

def cargar_config():
    cfg = dict(CONFIG_DEFAULT)
    try:
        if os.path.exists(ARCHIVO_CONFIG):
            with open(ARCHIVO_CONFIG, "r", encoding="utf-8") as f:
                cfg.update(json.load(f))
        else:
            with open(ARCHIVO_CONFIG, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print("No se pudo leer config.json, se usan valores por defecto: %s" % e)
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
        tmp = ruta + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(estado, f, indent=2, ensure_ascii=False)
        if os.path.exists(ruta):
            os.remove(ruta)
        os.rename(tmp, ruta)
    except Exception as e:
        log("No se pudo escribir el estado en Dropbox: %s" % e, "WARN")


def rango_dia(fecha):
    d = fecha.strftime("%Y-%m-%d")
    return "%s 00:00" % d, "%s 23:59" % d


def main():
    t0 = time.time()

    if os.path.exists(ARCHIVO_STOP):
        log("Archivo STOP presente. El agente no hace nada.")
        return 0

    orden = leer_orden()

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

    dias = [hoy - timedelta(days=i) for i in range(0, n_dias + 1)]

    total_cerrados = 0
    resumen_dia = {}

    # ---- boletos cerrados ----
    for idx, d in enumerate(dias):
        desde, hasta = rango_dia(d)
        crudos = leer_webservice(CFG["ws_cerrados"], desde, hasta)
        if crudos is None:
            continue
        fecha_k = d.strftime("%Y-%m-%d")
        registros = []
        for c in crudos:
            r = normalizar(c, abierto=False)
            if r:
                registros.append(r)
        for r in registros:
            enviar("boletos/%s/%d" % (fecha_k, r["boleto"]), r)
        total_cerrados += len(registros)

        # durante una carga larga, avisa por Dropbox como va
        if carga and (idx % 10 == 0 or idx == len(dias) - 1):
            vaciar_lote()
            escribir_estado({
                "agente": VERSION, "equipo": os.environ.get("COMPUTERNAME", "?"),
                "modo": "carga historica en curso", "orden": carga,
                "avance": "%d de %d dias" % (idx + 1, len(dias)),
                "pct": round((idx + 1) / len(dias) * 100),
                "boletos_hasta_ahora": total_cerrados,
                "peticiones": ENVIADOS[1],
                "ultima_corrida": datetime.now().isoformat(timespec="seconds"),
            })

        if registros:
            por_grupo = {}
            for r in registros:
                g = r.get("grupo") or "SIN PROCEDENCIA"
                a = por_grupo.setdefault(g, {"viajes": 0, "kg": 0})
                a["viajes"] += 1
                a["kg"] += (r.get("neto") or 0)
            resumen = {
                "fecha": fecha_k,
                "viajes": len(registros),
                "kg": sum(r.get("neto") or 0 for r in registros),
                "toneladas": round(sum(r.get("neto") or 0 for r in registros) / 1000.0, 3),
                "por_grupo": por_grupo,
                "actualizado": datetime.now().isoformat(timespec="seconds"),
            }
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
