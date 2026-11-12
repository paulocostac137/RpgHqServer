import asyncio
import json
import math
import os
import time

import websockets


# ============================================================
# CONFIGURAÇÕES
# ============================================================

HOST = "0.0.0.0"
PORTA = int(os.environ.get("PORT", 8765))

MAX_USUARIOS_POR_SALA = 2
TEMPO_LIMITE_ENTRADA = 30
TAMANHO_MAXIMO_MENSAGEM = 4096

# Cada sala contém seus participantes e seu controlador.
salas = {}


# ============================================================
# ESTRUTURA DAS SALAS
# ============================================================

def criar_sala():

    return {
        "participantes": [],
        "controlador": None,
        "sequencia": 0,
        "criada_em": time.time()
    }


def obter_sala(codigo):

    if codigo not in salas:
        salas[codigo] = criar_sala()

    return salas[codigo]


def atribuir_papel(sala):

    papeis_ocupados = {
        getattr(usuario, "papel", "")
        for usuario in sala["participantes"]
    }

    if "A" not in papeis_ocupados:
        return "A"

    if "B" not in papeis_ocupados:
        return "B"

    return None


# ============================================================
# ENVIO DE MENSAGENS
# ============================================================

async def enviar(usuario, mensagem):

    try:

        await usuario.send(
            json.dumps(
                mensagem,
                ensure_ascii=False,
                allow_nan=False
            )
        )

        return True

    except (
        websockets.exceptions.ConnectionClosed,
        TypeError,
        ValueError
    ):

        return False


async def enviar_para_par(usuario, mensagem):

    codigo = getattr(usuario, "sala", None)

    if codigo is None:
        return

    sala = salas.get(codigo)

    if sala is None:
        return

    for outro in list(sala["participantes"]):

        if outro != usuario:
            await enviar(outro, mensagem)


async def enviar_para_sala(codigo, mensagem):

    sala = salas.get(codigo)

    if sala is None:
        return

    for usuario in list(sala["participantes"]):
        await enviar(usuario, mensagem)


# ============================================================
# VALIDAÇÃO DOS DADOS
# ============================================================

def validar_progresso(valor):

    if isinstance(valor, bool):
        return None

    if not isinstance(valor, (int, float)):
        return None

    if not math.isfinite(valor):
        return None

    return max(0.0, min(1.0, float(valor)))


def validar_inteiro(valor, minimo=0, maximo=None):

    if isinstance(valor, bool):
        return None

    if not isinstance(valor, int):
        return None

    if valor < minimo:
        return None

    if maximo is not None and valor > maximo:
        return None

    return valor


# ============================================================
# CONTROLE EXCLUSIVO
# ============================================================

async def processar_controle(usuario, sala, dados):

    ativo = dados.get("active")

    if not isinstance(ativo, bool):
        return

    controlador = sala["controlador"]

    # --------------------------------------------------------
    # SOLICITAÇÃO OU ATUALIZAÇÃO DE MOVIMENTO
    # --------------------------------------------------------

    if ativo:

        progresso = validar_progresso(
            dados.get("progress")
        )

        if progresso is None:
            return

        # Ninguém controla a sala.
        if controlador is None:

            sala["controlador"] = usuario

            controlador = usuario

            await enviar(usuario, {
                "type": "control_granted",
                "role": usuario.papel
            })

            print(
                f"Controle concedido a {usuario.papel} "
                f"na sala {usuario.sala}."
            )

        # O próprio usuário já possui o controle.
        elif controlador != usuario:

            await enviar(usuario, {
                "type": "control_busy",
                "message": "O outro aparelho está controlando."
            })

            return

        sala["sequencia"] += 1

        await enviar_para_par(usuario, {
            "type": "control",
            "active": True,
            "progress": progresso,
            "role": usuario.papel,
            "sequence": sala["sequencia"],
            "server_time_ms": int(time.time() * 1000)
        })

        return

    # --------------------------------------------------------
    # LIBERAÇÃO DO CONTROLE
    # --------------------------------------------------------

    if controlador != usuario:

        # Um aparelho que não possui o controle
        # não pode liberar o controle do parceiro.
        return

    sala["controlador"] = None

    sala["sequencia"] += 1

    await enviar(usuario, {
        "type": "control_released"
    })

    await enviar_para_par(usuario, {
        "type": "control",
        "active": False,
        "role": usuario.papel,
        "sequence": sala["sequencia"],
        "server_time_ms": int(time.time() * 1000)
    })

    print(
        f"Controle liberado por {usuario.papel} "
        f"na sala {usuario.sala}."
    )


# ============================================================
# SINCRONIZAÇÃO DOS GIFS B, C E D
# ============================================================

ACOES_GIF = {
    "frame",
    "play",
    "pause",
    "select",
    "reset"
}

GIFS_PERMITIDOS = {
    "B",
    "C",
    "D"
}


async def processar_sincronizacao(usuario, sala, dados):

    # Somente o aparelho autorizado pode enviar
    # comandos de sincronização.
    if sala["controlador"] != usuario:
        return

    gif = dados.get("gif")
    acao = dados.get("action")

    # As duas validações abaixo impedem que valores
    # de outros tipos sejam utilizados nas verificações.
    if not isinstance(gif, str):
        return

    if not isinstance(acao, str):
        return

    if gif not in GIFS_PERMITIDOS:
        return

    if acao not in ACOES_GIF:
        return

    mensagem = {
        "type": "gif_sync",
        "gif": gif,
        "action": acao,
        "sequence": None,
        "role": usuario.papel,
        "server_time_ms": int(time.time() * 1000)
    }

    # Quadro atual, quando aplicável.
    if "frame" in dados:

        quadro = validar_inteiro(
            dados.get("frame")
        )

        if quadro is None:
            return

        mensagem["frame"] = quadro

    # Posição temporal do GIF.
    if "position_ms" in dados:

        posicao = validar_inteiro(
            dados.get("position_ms")
        )

        if posicao is None:
            return

        mensagem["position_ms"] = posicao

    # Duração total do GIF.
    if "duration_ms" in dados:

        duracao = validar_inteiro(
            dados.get("duration_ms"),
            minimo=1
        )

        if duracao is None:
            return

        mensagem["duration_ms"] = duracao

    # Estado de reprodução.
    if "playing" in dados:

        reproduzindo = dados.get("playing")

        if not isinstance(reproduzindo, bool):
            return

        mensagem["playing"] = reproduzindo

    sala["sequencia"] += 1

    mensagem["sequence"] = sala["sequencia"]

    await enviar_para_par(usuario, mensagem)


# ============================================================
# PROCESSAMENTO DAS MENSAGENS
# ============================================================

async def processar_mensagem(usuario, sala, mensagem):

    try:

        dados = json.loads(mensagem)

    except (json.JSONDecodeError, TypeError):

        return

    if not isinstance(dados, dict):
        return

    tipo = dados.get("type")

    if tipo == "control":

        await processar_controle(
            usuario,
            sala,
            dados
        )

    elif tipo == "gif_sync":

        await processar_sincronizacao(
            usuario,
            sala,
            dados
        )


# ============================================================
# CONEXÃO E ENTRADA NA SALA
# ============================================================

async def conectar(websocket):

    sala_usuario = None
    papel_usuario = None

    try:

        # Aguarda a identificação do aparelho.
        primeira_mensagem = await asyncio.wait_for(
            websocket.recv(),
            timeout=TEMPO_LIMITE_ENTRADA
        )

        try:

            dados = json.loads(primeira_mensagem)

        except (json.JSONDecodeError, TypeError):

            await enviar(websocket, {
                "type": "error",
                "message": "Mensagem inicial inválida."
            })

            return

        if not isinstance(dados, dict):

            await enviar(websocket, {
                "type": "error",
                "message": "Identificação inválida."
            })

            return

        if dados.get("type") != "join":

            await enviar(websocket, {
                "type": "error",
                "message": "Envie primeiro o código do par."
            })

            return

        codigo = str(
            dados.get("pair", "")
        ).strip()

        if not codigo or len(codigo) > 32:

            await enviar(websocket, {
                "type": "error",
                "message": "Código de conexão inválido."
            })

            return

        # Obtém ou cria a sala.
        sala = obter_sala(codigo)

        participantes = sala["participantes"]

        # Verifica o limite de usuários.
        if len(participantes) >= MAX_USUARIOS_POR_SALA:

            await enviar(websocket, {
                "type": "error",
                "message": "Este par já possui dois usuários."
            })

            return

        # Atribui o primeiro papel disponível.
        papel_usuario = atribuir_papel(sala)

        if papel_usuario is None:

            await enviar(websocket, {
                "type": "error",
                "message": "Não há posições disponíveis."
            })

            return

        # Registra o usuário.
        websocket.sala = codigo
        websocket.papel = papel_usuario

        participantes.append(websocket)

        sala_usuario = codigo

        # Informa sua identificação.
        await enviar(websocket, {
            "type": "joined",
            "pair": codigo,
            "role": papel_usuario,
            "users": len(participantes)
        })

        print(
            f"Usuário {papel_usuario} conectado "
            f"à sala {codigo}. "
            f"Participantes: {len(participantes)}"
        )

        # Quando o segundo usuário entra,
        # avisa o primeiro.
        if len(participantes) == 2:

            await enviar_para_par(websocket, {
                "type": "peer_connected",
                "users": 2
            })

        # ----------------------------------------------------
        # RECEBIMENTO CONTÍNUO DE MENSAGENS
        # ----------------------------------------------------

        async for mensagem in websocket:

            if not isinstance(mensagem, str):
                continue

            await processar_mensagem(
                websocket,
                sala,
                mensagem
            )

    except asyncio.TimeoutError:

        print(
            "Conexão encerrada por falta de identificação."
        )

        await enviar(websocket, {
            "type": "error",
            "message": "Tempo limite para entrar na sala."
        })

    except websockets.exceptions.ConnectionClosed:

        pass

    except Exception as erro:

        print(
            f"Erro na conexão: {erro}"
        )

    finally:

        # ----------------------------------------------------
        # DESCONECTAR E LIMPAR A SALA
        # ----------------------------------------------------

        if sala_usuario is not None:

            sala = salas.get(sala_usuario)

            if sala is not None:

                participantes = sala["participantes"]

                if websocket in participantes:

                    era_controlador = (
                        sala["controlador"] == websocket
                    )

                    participantes.remove(websocket)

                    # Libera o controle se o controlador saiu.
                    if era_controlador:

                        sala["controlador"] = None

                        sala["sequencia"] += 1

                        await enviar_para_sala(
                            sala_usuario,
                            {
                                "type": "control",
                                "active": False,
                                "role": papel_usuario,
                                "sequence": sala["sequencia"],
                                "server_time_ms": int(
                                    time.time() * 1000
                                )
                            }
                        )

                        await enviar_para_sala(
                            sala_usuario,
                            {
                                "type": "control_released"
                            }
                        )

                    # Informa a saída do parceiro.
                    await enviar_para_sala(
                        sala_usuario,
                        {
                            "type": "peer_disconnected",
                            "role": papel_usuario
                        }
                    )

                    print(
                        f"Usuário {papel_usuario} "
                        f"desconectado da sala {sala_usuario}."
                    )

                    # Remove a sala quando estiver vazia.
                    if not participantes:

                        salas.pop(sala_usuario, None)

                        print(
                            f"Sala {sala_usuario} encerrada."
                        )


# ============================================================
# INICIALIZAÇÃO
# ============================================================

async def main():

    print("----------------------------------------")
    print("       RPGHQCONSPIRATION SERVER")
    print("----------------------------------------")

    print(f"Host: {HOST}")
    print(f"Porta: {PORTA}")
    print("Protocolo: WebSocket")
    print("Sincronização: GIF B, C e D")
    print("----------------------------------------")

    async with websockets.serve(
        conectar,
        HOST,
        PORTA,
        ping_interval=20,
        ping_timeout=20,
        max_size=TAMANHO_MAXIMO_MENSAGEM
    ):

        print("Servidor iniciado.")
        print("Aguardando usuários...")

        await asyncio.Future()


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print("Servidor encerrado.")