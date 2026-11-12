import asyncio
import json
import os
import websockets

# =====================================================
# CONFIGURAÇÕES
# =====================================================

HOST = "0.0.0.0"
PORTA = int(os.environ.get("PORT", 8765))

# Cada sala contém:
# {
#   "users": [websocket_A, websocket_B],
#   "controller": websocket_que_tem_o_controle
# }
salas = {}


# =====================================================
# ENVIO DE MENSAGENS
# =====================================================

async def enviar(usuario, mensagem):
    try:
        await usuario.send(json.dumps(mensagem))
    except websockets.exceptions.ConnectionClosed:
        pass


async def enviar_para_sala(codigo, mensagem, incluir_remetente=True):
    sala = salas.get(codigo)
    if sala is None:
        return

    for usuario in list(sala["users"]):
        if incluir_remetente or usuario is not mensagem.get("_sender"):
            await enviar(usuario, mensagem)


async def avisar_parceiro(codigo, remetente, mensagem):
    sala = salas.get(codigo)
    if sala is None:
        return

    for outro in list(sala["users"]):
        if outro is not remetente:
            await enviar(outro, mensagem)


# =====================================================
# CONEXÃO DOS USUÁRIOS
# =====================================================

async def conectar(websocket):
    codigo_sala = None

    try:
        # A primeira mensagem obrigatoriamente identifica a sala.
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

        sala = salas.setdefault(codigo, {
            "users": [],
            "controller": None
        })

        if len(sala["users"]) >= 2:
            await enviar(websocket, {
                "type": "error",
                "message": "Este par já possui dois usuários."
            })
            return

        # A ordem de entrada determina os papéis.
        papel = "A" if len(sala["users"]) == 0 else "B"

        sala["users"].append(websocket)
        websocket.sala = codigo
        websocket.papel = papel
        codigo_sala = codigo

        await enviar(websocket, {
            "type": "joined",
            "pair": codigo,
            "users": len(sala["users"]),
            "role": papel
        })

        print(
            f"Usuário {papel} conectado à sala {codigo}. "
            f"Participantes: {len(sala['users'])}"
        )

        # Quando o segundo usuário entra, avisa o primeiro.
        if len(sala["users"]) == 2:
            await avisar_parceiro(codigo, websocket, {
                "type": "peer_connected",
                "role": papel
            })

        # Recebe os comandos enviados durante a sessão.
        async for mensagem in websocket:
            try:
                dados = json.loads(mensagem)

                if dados.get("type") != "control":
                    continue

                ativo = dados.get("active")

                if not isinstance(ativo, bool):
                    continue

                sala_atual = salas.get(codigo)
                if sala_atual is None:
                    continue

                controlador = sala_atual["controller"]

                if ativo:
                    progresso = dados.get("progress")

                    if (
                        not isinstance(progresso, (int, float))
                        or isinstance(progresso, bool)
                    ):
                        continue

                    progresso = max(0.0, min(1.0, float(progresso)))

                    # O primeiro usuário que solicitar assume o controle.
                    if controlador is None:
                        sala_atual["controller"] = websocket
                        controlador = websocket

                        await enviar(websocket, {
                            "type": "control_granted",
                            "role": websocket.papel
                        })

                        print(
                            f"Controle concedido ao usuário "
                            f"{websocket.papel} na sala {codigo}."
                        )

                    # O usuário que já controla pode continuar enviando.
                    if controlador is websocket:
                        await avisar_parceiro(codigo, websocket, {
                            "type": "control",
                            "active": True,
                            "progress": progresso,
                            "role": websocket.papel
                        })
                    else:
                        # Não permite que o segundo usuário interrompa o dono.
                        await enviar(websocket, {
                            "type": "control_busy",
                            "role": getattr(controlador, "papel", "")
                        })

                else:
                    # Só o dono atual pode liberar a trava.
                    if sala_atual["controller"] is websocket:
                        sala_atual["controller"] = None

                        await avisar_parceiro(codigo, websocket, {
                            "type": "control",
                            "active": False,
                            "role": websocket.papel
                        })

                        await enviar(websocket, {
                            "type": "control_released"
                        })

                        print(
                            f"Controle liberado pelo usuário "
                            f"{websocket.papel} na sala {codigo}."
                        )

            except (json.JSONDecodeError, TypeError, ValueError):
                continue

    except asyncio.TimeoutError:
        print("Conexão encerrada por falta de identificação.")

    except websockets.exceptions.ConnectionClosed:
        pass

    finally:
        if codigo_sala is not None:
            sala = salas.get(codigo_sala)

            if sala is not None:
                # Se quem desconectou era o controlador, libera a trava.
                if sala["controller"] is websocket:
                    sala["controller"] = None

                    for outro in list(sala["users"]):
                        if outro is not websocket:
                            await enviar(outro, {
                                "type": "control",
                                "active": False,
                                "role": websocket.papel
                            })

                if websocket in sala["users"]:
                    sala["users"].remove(websocket)

                for outro in list(sala["users"]):
                    await enviar(outro, {
                        "type": "peer_disconnected",
                        "role": websocket.papel
                    })

                if not sala["users"]:
                    salas.pop(codigo_sala, None)

            print(
                f"Usuário desconectado da sala {codigo_sala}."
            )


# =====================================================
# INICIALIZAÇÃO DO SERVIDOR
# =====================================================

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