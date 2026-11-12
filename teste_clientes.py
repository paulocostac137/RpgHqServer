import asyncio
import json
import websockets

SERVIDOR = "ws://127.0.0.1:8765"


async def receber(websocket):
    mensagem = await asyncio.wait_for(
        websocket.recv(),
        timeout=5
    )
    return json.loads(mensagem)


async def entrar(websocket, codigo):
    await websocket.send(json.dumps({
        "type": "join",
        "pair": codigo
    }))
    return await receber(websocket)


async def testar():

    print("\n=== TESTE DO RPGHQ SERVER ===\n")

    # TESTE 1: Primeiro usuário
    async with websockets.connect(SERVIDOR) as usuario_a:

        resposta = await entrar(usuario_a, "123456")

        assert resposta["type"] == "joined"
        assert resposta["users"] == 1

        print("[OK] Primeiro usuário conectado.")

        # TESTE 2: Segundo usuário no mesmo ID
        async with websockets.connect(SERVIDOR) as usuario_b:

            resposta = await entrar(usuario_b, "123456")

            assert resposta["type"] == "joined"
            assert resposta["users"] == 2

            aviso = await receber(usuario_a)

            assert aviso["type"] == "peer_connected"

            print("[OK] Segundo usuário pareado.")
            print("[OK] Primeiro usuário recebeu aviso.")

            # TESTE 3: Terceiro usuário na mesma sala
            async with websockets.connect(SERVIDOR) as usuario_c:

                resposta = await entrar(usuario_c, "123456")

                assert resposta["type"] == "error"

                print("[OK] Terceiro usuário recusado.")

            # TESTE 4: Comunicação A -> B
            await usuario_a.send(json.dumps({
                "type": "control",
                "active": True,
                "progress": 0.75
            }))

            resposta = await receber(usuario_b)

            assert resposta["type"] == "control"
            assert resposta["active"] is True
            assert resposta["progress"] == 0.75

            print("[OK] Comunicação A -> B.")

            # TESTE 5: Comunicação B -> A
            await usuario_b.send(json.dumps({
                "type": "control",
                "active": False
            }))

            resposta = await receber(usuario_a)

            assert resposta["type"] == "control"
            assert resposta["active"] is False

            print("[OK] Comunicação B -> A.")

            # TESTE 6: Outra sala independente
            async with websockets.connect(SERVIDOR) as usuario_d:

                resposta = await entrar(usuario_d, "654321")

                assert resposta["type"] == "joined"
                assert resposta["users"] == 1

                print("[OK] Outra sala criada independentemente.")

            # TESTE 7: Desconexão
            await usuario_b.close()

            resposta = await receber(usuario_a)

            assert resposta["type"] == "peer_disconnected"

            print("[OK] Desconexão comunicada ao parceiro.")

    print("\n=== TODOS OS TESTES PASSARAM ===")


if __name__ == "__main__":
    try:
        asyncio.run(testar())
    except Exception as erro:
        print("\n[ERRO NO TESTE]")
        print(type(erro).__name__, erro)