import asyncio
import json
import os
import websockets

HOST = "0.0.0.0"
PORTA = int(os.environ.get("PORT", 8765))

# Cada sala mantém os usuários na ordem de entrada.
# O primeiro recebe A e o segundo recebe B.
salas = {}
lock_salas = asyncio.Lock()


async def enviar(usuario, mensagem):
    try:
        await usuario.send(json.dumps(mensagem))
    except websockets.exceptions.ConnectionClosed:
        pass


async def enviar_para_sala(sala, mensagem, exceto=None):
    estado = salas.get(sala)

    if not estado:
        return

    for usuario in list(estado["usuarios"]):
        if usuario is not exceto:
            await enviar(usuario, mensagem)


async def conectar(websocket):

    sala_usuario = None
    papel_usuario = None
    restantes = []

    try:
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

        async with lock_salas:

            estado = salas.setdefault(
                codigo,
                {
                    "usuarios": [],
                    "controlador": None
                }
            )

            if len(estado["usuarios"]) >= 2:
                await enviar(websocket, {
                    "type": "error",
                    "message": "Este par já possui dois usuários."
                })
                return

            papel_usuario = (
                "A" if not estado["usuarios"] else "B"
            )

            estado["usuarios"].append(websocket)

            websocket.sala = codigo
            websocket.papel = papel_usuario

            sala_usuario = codigo
            quantidade = len(estado["usuarios"])

        await enviar(websocket, {
            "type": "joined",
            "pair": codigo,
            "users": quantidade,
            "role": papel_usuario
        })

        print(
            f"Usuário {papel_usuario} conectado à sala {codigo}. "
            f"Participantes: {quantidade}"
        )

        if quantidade == 2:

            await enviar_para_sala(
                codigo,
                {"type": "peer_connected"}
            )

        async for mensagem in websocket:

            try:
                dados = json.loads(mensagem)
            except (json.JSONDecodeError, TypeError):
                continue

            if dados.get("type") != "control":
                continue

            ativo = dados.get("active")

            if not isinstance(ativo, bool):
                continue

            async with lock_salas:

                estado = salas.get(codigo)

                if not estado or websocket not in estado["usuarios"]:
                    continue

                controlador = estado["controlador"]

                if ativo:

                    progresso = dados.get("progress")

                    if (
                        not isinstance(progresso, (int, float))
                        or isinstance(progresso, bool)
                    ):
                        continue

                    if controlador is None:

                        estado["controlador"] = websocket
                        controlador = websocket

                        await enviar(websocket, {
                            "type": "control_granted",
                            "role": papel_usuario
                        })

                    elif controlador is not websocket:

                        await enviar(websocket, {
                            "type": "control_busy",
                            "controller": getattr(
                                controlador,
                                "papel",
                                ""
                            )
                        })

                        continue

                    resposta = {
                        "type": "control",
                        "active": True,
                        "progress": max(
                            0.0,
                            min(1.0, float(progresso))
                        ),
                        "controller": papel_usuario
                    }

                    await enviar_para_sala(
                        codigo,
                        resposta,
                        exceto=websocket
                    )

                else:

                    if controlador is websocket:

                        estado["controlador"] = None

                        resposta = {
                            "type": "control",
                            "active": False,
                            "controller": papel_usuario
                        }

                        await enviar_para_sala(
                            codigo,
                            resposta,
                            exceto=websocket
                        )

                        await enviar(websocket, {
                            "type": "control_released"
                        })

    except asyncio.TimeoutError:

        print(
            "Conexão encerrada por falta de identificação."
        )

    except websockets.exceptions.ConnectionClosed:
        pass

    except Exception as erro:

        print(f"Erro na conexão: {erro}")

        await enviar(websocket, {
            "type": "error",
            "message": "Erro interno no servidor."
        })

    finally:

        if sala_usuario is not None:

            async with lock_salas:

                estado = salas.get(sala_usuario)

                if estado and websocket in estado["usuarios"]:

                    if estado["controlador"] is websocket:

                        estado["controlador"] = None

                        await enviar_para_sala(
                            sala_usuario,
                            {
                                "type": "control",
                                "active": False,
                                "controller": papel_usuario
                            },
                            exceto=websocket
                        )

                    estado["usuarios"].remove(websocket)

                    restantes = list(estado["usuarios"])

                    if not restantes:
                        salas.pop(sala_usuario, None)

            for outro in restantes:

                await enviar(outro, {
                    "type": "peer_disconnected",
                    "role": papel_usuario
                })

            print(
                f"Usuário {papel_usuario} desconectado "
                f"da sala {sala_usuario}."
            )


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