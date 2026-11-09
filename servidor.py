import asyncio
import json
import os
import websockets

# =========================================
# CONFIGURAÇÕES
# =========================================

HOST = "0.0.0.0"
PORTA = int(os.environ.get("PORT", 8765))

# Cada código identifica um par de usuários.
salas = {}


# =========================================
# ENVIO DE MENSAGENS
# =========================================

async def enviar(usuario, mensagem):
    try:
        await usuario.send(json.dumps(mensagem))
    except websockets.exceptions.ConnectionClosed:
        pass


async def encaminhar_para_par(usuario, mensagem):
    sala = getattr(usuario, "sala", None)

    if sala is None:
        return

    participantes = salas.get(sala, set())

    for outro in list(participantes):
        if outro != usuario:
            await enviar(outro, mensagem)


# =========================================
# CONEXÃO DOS USUÁRIOS
# =========================================

async def conectar(websocket):

    sala_usuario = None

    try:
        # Aguarda identificação do usuário.
        primeira_mensagem = await asyncio.wait_for(
            websocket.recv(),
            timeout=30
        )

        dados = json.loads(primeira_mensagem)

        if dados.get("type") != "join":
            await enviar(websocket, {
                "type": "error",
                "message": "Envie primeiro o código do par."
            })
            return

        codigo = str(dados.get("pair", "")).strip()

        if not codigo or len(codigo) > 32:
            await enviar(websocket, {
                "type": "error",
                "message": "Código de conexão inválido."
            })
            return

        # Cria a sala, se necessário.
        participantes = salas.setdefault(codigo, set())

        # Limite de dois usuários por sala.
        if len(participantes) >= 2:
            await enviar(websocket, {
                "type": "error",
                "message": "Este par já possui dois usuários."
            })
            return

        participantes.add(websocket)
        websocket.sala = codigo
        sala_usuario = codigo

        await enviar(websocket, {
            "type": "joined",
            "pair": codigo,
            "users": len(participantes)
        })

        print(
            f"Usuário conectado à sala {codigo}. "
            f"Participantes: {len(participantes)}"
        )

        # Informa ao outro usuário que o par entrou.
        if len(participantes) == 2:
            await encaminhar_para_par(websocket, {
                "type": "peer_connected"
            })

        # Recebe e encaminha os comandos.
        async for mensagem in websocket:

            try:
                dados = json.loads(mensagem)

                if dados.get("type") != "control":
                    continue

                ativo = dados.get("active")

                if not isinstance(ativo, bool):
                    continue

                resposta = {
                    "type": "control",
                    "active": ativo
                }

                if ativo:
                    progresso = dados.get("progress")

                    if (
                        not isinstance(progresso, (int, float))
                        or isinstance(progresso, bool)
                    ):
                        continue

                    resposta["progress"] = max(
                        0.0,
                        min(1.0, float(progresso))
                    )

                await encaminhar_para_par(
                    websocket,
                    resposta
                )

            except (json.JSONDecodeError, TypeError):
                continue

    except asyncio.TimeoutError:
        print("Conexão encerrada por falta de identificação.")

    except websockets.exceptions.ConnectionClosed:
        pass

    finally:

        # Remove o usuário da sala.
        if sala_usuario is not None:

            participantes = salas.get(sala_usuario)

            if participantes is not None:

                participantes.discard(websocket)

                for outro in list(participantes):
                    await enviar(outro, {
                        "type": "peer_disconnected"
                    })

                if not participantes:
                    salas.pop(sala_usuario, None)

            print(
                f"Usuário desconectado da sala {sala_usuario}."
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