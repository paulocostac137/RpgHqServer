import asyncio
import json
import os
import websockets

# =====================================================
# CONFIGURAÇÕES
# =====================================================

HOST = "0.0.0.0"
PORTA = int(os.environ.get("PORT", 8765))

# Cada código identifica uma sala com, no máximo, dois aparelhos.
# A lista preserva a ordem de entrada dos usuários.
salas = {}

# Guarda quem está controlando cada sala.
controladores = {}


# =====================================================
# ENVIO DE MENSAGENS
# =====================================================

async def enviar(usuario, mensagem):
    try:
        await usuario.send(json.dumps(mensagem))
    except websockets.exceptions.ConnectionClosed:
        pass


async def enviar_para_par(usuario, mensagem):
    sala = getattr(usuario, "sala", None)

    if sala is None:
        return

    participantes = salas.get(sala, [])

    for outro in list(participantes):
        if outro != usuario:
            await enviar(outro, mensagem)


async def enviar_para_sala(sala, mensagem):
    participantes = salas.get(sala, [])

    for usuario in list(participantes):
        await enviar(usuario, mensagem)


# =====================================================
# ENTRADA NA SALA
# =====================================================

async def conectar(websocket):

    sala_usuario = None
    papel_usuario = None

    try:

        # Aguarda a identificação do aparelho.
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

        codigo = str(
            dados.get("pair", "")
        ).strip()

        if not codigo or len(codigo) > 32:

            await enviar(websocket, {
                "type": "error",
                "message": "Código de conexão inválido."
            })

            return

        # Cria a sala, se ela ainda não existir.
        participantes = salas.setdefault(codigo, [])

        # Limite de dois aparelhos.
        if len(participantes) >= 2:

            await enviar(websocket, {
                "type": "error",
                "message": "Este par já possui dois usuários."
            })

            return

        # Define o papel conforme a ordem de entrada.
        if len(participantes) == 0:
            papel_usuario = "A"
        else:
            papel_usuario = "B"

        participantes.append(websocket)

        websocket.sala = codigo
        websocket.papel = papel_usuario

        sala_usuario = codigo

        # Informa ao aparelho seu papel.
        await enviar(websocket, {
            "type": "joined",
            "pair": codigo,
            "role": papel_usuario,
            "users": len(participantes)
        })

        print(
            f"Usuário {papel_usuario} conectado à sala {codigo}. "
            f"Participantes: {len(participantes)}"
        )

        # Quando B entra, A recebe o aviso.
        if len(participantes) == 2:

            await enviar_para_par(websocket, {
                "type": "peer_connected",
                "users": 2
            })

        # =================================================
        # RECEBIMENTO DE COMANDOS
        # =================================================

        async for mensagem in websocket:

            try:

                dados = json.loads(mensagem)

                tipo = dados.get("type")

                if tipo != "control":
                    continue

                ativo = dados.get("active")

                if not isinstance(ativo, bool):
                    continue

                participantes = salas.get(codigo, [])

                if websocket not in participantes:
                    continue

                controlador_atual = controladores.get(codigo)

                # =========================================
                # SOLICITAÇÃO DE CONTROLE
                # =========================================

                if ativo:

                    progresso = dados.get("progress")

                    if (
                        not isinstance(progresso, (int, float))
                        or isinstance(progresso, bool)
                    ):
                        continue

                    progresso = max(
                        0.0,
                        min(1.0, float(progresso))
                    )

                    # Ninguém está controlando:
                    # concede o controle ao solicitante.
                    if controlador_atual is None:

                        controladores[codigo] = websocket

                        await enviar(websocket, {
                            "type": "control_granted",
                            "role": papel_usuario
                        })

                        print(
                            f"Controle concedido ao usuário "
                            f"{papel_usuario} na sala {codigo}."
                        )

                        # Envia o primeiro estado ao parceiro.
                        await enviar_para_par(
                            websocket,
                            {
                                "type": "control",
                                "active": True,
                                "progress": progresso,
                                "role": papel_usuario
                            }
                        )

                    # O próprio controlador está enviando progresso.
                    elif controlador_atual == websocket:

                        await enviar_para_par(
                            websocket,
                            {
                                "type": "control",
                                "active": True,
                                "progress": progresso,
                                "role": papel_usuario
                            }
                        )

                    # Outro aparelho já está controlando.
                    else:

                        await enviar(websocket, {
                            "type": "control_busy",
                            "message": "O outro aparelho está controlando."
                        })

                # =========================================
                # LIBERAÇÃO DO CONTROLE
                # =========================================

                else:

                    # Somente quem possui o controle pode liberá-lo.
                    if controlador_atual == websocket:

                        controladores.pop(codigo, None)

                        print(
                            f"Controle liberado pelo usuário "
                            f"{papel_usuario} na sala {codigo}."
                        )

                        await enviar(websocket, {
                            "type": "control_released"
                        })

                        await enviar_para_par(
                            websocket,
                            {
                                "type": "control",
                                "active": False,
                                "role": papel_usuario
                            }
                        )

            except (
                json.JSONDecodeError,
                TypeError,
                ValueError
            ):

                continue

    except asyncio.TimeoutError:

        print(
            "Conexão encerrada por falta de identificação."
        )

    except websockets.exceptions.ConnectionClosed:

        pass

    finally:

        # =================================================
        # DESCONECTAR E LIMPAR A SALA
        # =================================================

        if sala_usuario is not None:

            participantes = salas.get(sala_usuario, [])

            if websocket in participantes:

                era_controlador = (
                    controladores.get(sala_usuario) == websocket
                )

                participantes.remove(websocket)

                # Se o controlador saiu, libera o controle.
                if era_controlador:

                    controladores.pop(sala_usuario, None)

                    await enviar_para_sala(
                        sala_usuario,
                        {
                            "type": "control_released"
                        }
                    )

                # Avisa o parceiro que houve desconexão.
                await enviar_para_sala(
                    sala_usuario,
                    {
                        "type": "peer_disconnected"
                    }
                )

                print(
                    f"Usuário {papel_usuario} desconectado "
                    f"da sala {sala_usuario}."
                )

                # Remove a sala quando ficar vazia.
                if not participantes:

                    salas.pop(sala_usuario, None)
                    controladores.pop(sala_usuario, None)


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