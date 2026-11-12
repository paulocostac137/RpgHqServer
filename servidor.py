import asyncio
import json
import math
import os
import websockets

# =========================================
# CONFIGURAÇÕES
# =========================================

HOST = "0.0.0.0"
PORTA = int(os.environ.get("PORT", 8765))

# Participantes de cada sala.
salas = {}

# Último estado de controle recebido em cada sala.
estados_controle = {}


# =========================================
# ENVIO DE MENSAGENS
# =========================================

async def enviar(usuario, mensagem):
    try:
        await usuario.send(json.dumps(mensagem))

    except websockets.exceptions.ConnectionClosed:
        pass


async def encaminhar_para_par(usuario, mensagem):

    codigo = getattr(usuario, "sala", None)

    if codigo is None:
        return

    participantes = salas.get(codigo, set())

    for outro in list(participantes):

        if outro != usuario:
            await enviar(outro, mensagem)


# =========================================
# PROCESSAMENTO DOS CONTROLES
# =========================================

def validar_controle(dados):

    ativo = dados.get("active")

    if not isinstance(ativo, bool):
        return None

    resposta = {
        "type": "control",
        "active": ativo
    }

    if ativo:

        progresso = dados.get("progress")

        if (
            not isinstance(progresso, (int, float))
            or isinstance(progresso, bool)
            or not math.isfinite(progresso)
        ):
            return None

        resposta["progress"] = max(
            0.0,
            min(1.0, float(progresso))
        )

    return resposta


async def processar_controle(usuario, dados):

    codigo = getattr(usuario, "sala", None)

    if codigo is None:
        return

    resposta = validar_controle(dados)

    if resposta is None:
        return

    # Guarda o último estado conhecido da sala.
    estados_controle[codigo] = resposta

    # Encaminha para o outro dispositivo.
    await encaminhar_para_par(usuario, resposta)


# =========================================
# CONEXÃO DOS USUÁRIOS
# =========================================

async def conectar(websocket):

    sala_usuario = None

    try:

        # Aguarda o código de identificação.
        primeira_mensagem = await asyncio.wait_for(
            websocket.recv(),
            timeout=30
        )

        dados = json.loads(primeira_mensagem)

        if not isinstance(dados, dict):
            await enviar(websocket, {
                "type": "error",
                "message": "Mensagem de identificação inválida."
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

        # Cria a sala, caso ainda não exista.
        participantes = salas.setdefault(
            codigo,
            set()
        )

        # Limite de dois participantes.
        if len(participantes) >= 2:

            await enviar(websocket, {
                "type": "error",
                "message": "Este par já possui dois usuários."
            })

            return

        # Registra o participante.
        participantes.add(websocket)

        websocket.sala = codigo
        sala_usuario = codigo

        quantidade = len(participantes)

        print(
            f"Usuário conectado à sala {codigo}. "
            f"Participantes: {quantidade}"
        )

        # Confirma a entrada.
        await enviar(websocket, {
            "type": "joined",
            "pair": codigo,
            "users": quantidade
        })

        # Se já existe um estado de controle,
        # envia esse estado ao usuário que acabou de entrar.
        estado = estados_controle.get(codigo)

        if estado is not None:
            await enviar(websocket, estado)

        # Avisa o primeiro participante
        # que o segundo acabou de entrar.
        if quantidade == 2:

            await encaminhar_para_par(
                websocket,
                {
                    "type": "peer_connected"
                }
            )

        # =================================
        # RECEBIMENTO DAS MENSAGENS
        # =================================

        async for mensagem in websocket:

            try:

                dados = json.loads(mensagem)

                if not isinstance(dados, dict):
                    continue

                if dados.get("type") == "control":

                    await processar_controle(
                        websocket,
                        dados
                    )

            except (
                json.JSONDecodeError,
                TypeError,
                ValueError
            ):

                print(
                    f"Mensagem inválida recebida "
                    f"na sala {codigo}."
                )

                continue

    except asyncio.TimeoutError:

        print(
            "Conexão encerrada por falta de identificação."
        )

    except websockets.exceptions.ConnectionClosed:

        pass

    except Exception as erro:

        print(
            f"Erro na conexão: {erro}"
        )

    finally:

        # =================================
        # DESCONECTAR PARTICIPANTE
        # =================================

        if sala_usuario is not None:

            participantes = salas.get(
                sala_usuario
            )

            if participantes is not None:

                participantes.discard(websocket)

                # Avisa o participante restante.
                for outro in list(participantes):

                    await enviar(outro, {
                        "type": "peer_disconnected"
                    })

                # Evita reutilizar um estado antigo
                # quando um novo parceiro entrar.
                estados_controle.pop(
                    sala_usuario,
                    None
                )

                # Exclui a sala quando estiver vazia.
                if not participantes:

                    salas.pop(
                        sala_usuario,
                        None
                    )

            print(
                f"Usuário desconectado da sala "
                f"{sala_usuario}."
            )


# =========================================
# INICIALIZAÇÃO DO SERVIDOR
# =========================================

async def main():

    print("--------------------------------")
    print(" RPGHQCONSPIRATION - SERVIDOR")
    print("--------------------------------")
    print(f"Servidor iniciado na porta {PORTA}")
    print("Aguardando usuários...")
    print("--------------------------------")

    async with websockets.serve(
        conectar,
        HOST,
        PORTA,
        ping_interval=20,
        ping_timeout=20,
        max_size=4096
    ):

        await asyncio.Future()


if __name__ == "__main__":

    asyncio.run(main())