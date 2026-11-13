import asyncio
import json
import os
import websockets

# =========================================
# CONFIGURAÇÕES
# =========================================

HOST = "0.0.0.0"
PORTA = int(os.environ.get("PORT", 8765))

LIMITE_CICLOS_LIBERACAO = 5
LIMITE_CICLOS_SOLTAR = 3

LIMITE_EXTREMO_SUPERIOR = 0.15
LIMITE_EXTREMO_INFERIOR = 0.85

salas = {}
estados_salas = {}


# =========================================
# ESTADO DAS SALAS
# =========================================

def criar_estado_sala():
    return {
        "ciclos_a": 0,
        "extremo_inicial_a": -1,
        "alcancou_oposto_a": False,
        "usuario_b_liberado": False
    }


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
# LIBERAÇÃO DOS CONTROLES DE B
# =========================================

async def liberar_usuario_b(codigo):
    estado = estados_salas.get(codigo)

    if estado is None:
        return

    if estado["usuario_b_liberado"]:
        return

    estado["usuario_b_liberado"] = True

    participantes = salas.get(codigo, set())

    for usuario in list(participantes):
        if getattr(usuario, "papel_usuario", "") == "B":
            await enviar(usuario, {
                "type": "unlock_b"
            })

    print(
        f"Controles do usuário B liberados na sala {codigo}."
    )


# =========================================
# CONTAGEM DOS MOVIMENTOS DE A
# =========================================

async def registrar_movimento_usuario_a(
    codigo,
    progresso
):
    estado = estados_salas.get(codigo)

    if estado is None:
        return

    if estado["usuario_b_liberado"]:
        return

    extremo_atual = -1

    if progresso <= LIMITE_EXTREMO_SUPERIOR:
        extremo_atual = 0

    elif progresso >= LIMITE_EXTREMO_INFERIOR:
        extremo_atual = 1

    if extremo_atual == -1:
        return

    if estado["extremo_inicial_a"] == -1:

        estado["extremo_inicial_a"] = extremo_atual
        return

    if (
        not estado["alcancou_oposto_a"]
        and extremo_atual != estado["extremo_inicial_a"]
    ):

        estado["alcancou_oposto_a"] = True
        return

    if (
        estado["alcancou_oposto_a"]
        and extremo_atual == estado["extremo_inicial_a"]
    ):

        estado["ciclos_a"] += 1

        estado["alcancou_oposto_a"] = False

        print(
            f"Sala {codigo}: "
            f"A completou {estado['ciclos_a']} movimentos."
        )

        # Cinco movimentos completos:
        # libera B imediatamente, mesmo com os botões pressionados.

        if estado["ciclos_a"] >= LIMITE_CICLOS_LIBERACAO:

            await liberar_usuario_b(codigo)


# =========================================
# CONEXÃO DOS USUÁRIOS
# =========================================

async def conectar(websocket):

    sala_usuario = None

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

        codigo = str(
            dados.get("pair", "")
        ).strip()

        if not codigo or len(codigo) > 32:

            await enviar(websocket, {
                "type": "error",
                "message": "Código de conexão inválido."
            })

            return

        participantes = salas.setdefault(
            codigo,
            set()
        )

        if len(participantes) >= 2:

            await enviar(websocket, {
                "type": "error",
                "message": "Este par já possui dois usuários."
            })

            return

        # A ordem de entrada determina o papel.

        papel = "A" if len(participantes) == 0 else "B"

        websocket.papel_usuario = papel
        websocket.sala = codigo

        participantes.add(websocket)

        sala_usuario = codigo

        if codigo not in estados_salas:
            estados_salas[codigo] = criar_estado_sala()

        await enviar(websocket, {
            "type": "joined",
            "pair": codigo,
            "users": len(participantes),
            "role": papel
        })

        print(
            f"Usuário {papel} conectado à sala {codigo}. "
            f"Participantes: {len(participantes)}"
        )

        if len(participantes) == 2:

            await encaminhar_para_par(
                websocket,
                {
                    "type": "peer_connected"
                }
            )

        # =================================
        # RECEBIMENTO DOS COMANDOS
        # =================================

        async for mensagem in websocket:

            try:

                dados = json.loads(mensagem)

                tipo = dados.get("type")

                if tipo != "control":
                    continue

                ativo = dados.get("active")

                if not isinstance(ativo, bool):
                    continue

                resposta = {
                    "type": "control",
                    "active": ativo
                }

                progresso = None

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

                    resposta["progress"] = progresso

                # Encaminha o movimento normalmente.

                await encaminhar_para_par(
                    websocket,
                    resposta
                )

                # Somente A pode liberar os controles de B.

                if papel == "A":

                    if ativo:

                        await registrar_movimento_usuario_a(
                            codigo,
                            progresso
                        )

                    else:

                        estado = estados_salas.get(codigo)

                        if estado is not None:

                            # Três movimentos completos e soltura:
                            # libera B.

                            if (
                                estado["ciclos_a"]
                                >= LIMITE_CICLOS_SOLTAR
                            ):

                                await liberar_usuario_b(codigo)

                            else:

                                # Movimento insuficiente:
                                # reinicia a contagem para a próxima tentativa.

                                estado["ciclos_a"] = 0
                                estado["extremo_inicial_a"] = -1
                                estado["alcancou_oposto_a"] = False

            except (
                json.JSONDecodeError,
                TypeError
            ):

                continue

    except asyncio.TimeoutError:

        print(
            "Conexão encerrada por falta de identificação."
        )

    except websockets.exceptions.ConnectionClosed:

        pass

    finally:

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
                    estados_salas.pop(sala_usuario, None)

            print(
                f"Usuário desconectado da sala {sala_usuario}."
            )


# =========================================
# INICIALIZAÇÃO
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