import asyncio
import inspect
import json
from collections import deque

from loguru import logger
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, GiftEvent

from .conversations.single_conversation import process_single_conversation


class TikTokBridge:
    GIFT_MOTIONS = {
        "finger heart": "Dance",
        "friendship necklace": "Walk",
        "tiny diny": "Jump",
    }

    def __init__(self, ws_handler):
        self.ws_handler = ws_handler
        self.client = None
        self.listen_task = None
        self.worker_task = None
        self.username = None
        self.queue = asyncio.Queue(maxsize=20)
        self.seen = set()
        self.seen_order = deque()

    async def connect(self, username):
        username = str(username or "").strip().lstrip("@")
        if not username:
            return {"ok": False, "error": "Informe o usuario do TikTok."}

        await self.disconnect()

        self.username = username
        client = TikTokLiveClient(unique_id=username)
        self.client = client

        @client.on(CommentEvent)
        async def on_comment(event):
            comment = str(getattr(event, "comment", "") or "").strip()
            user = getattr(event, "user", None)
            nickname = (
                getattr(user, "nickname", None)
                or getattr(user, "unique_id", None)
                or "Espectador"
            )

            if not comment:
                return

            identity = str(
                getattr(user, "unique_id", None) or nickname
            ).lower()
            key = (identity, comment.lower())

            if key in self.seen:
                return

            if len(self.seen_order) >= 1000:
                old = self.seen_order.popleft()
                self.seen.discard(old)

            self.seen.add(key)
            self.seen_order.append(key)

            try:
                self.queue.put_nowait((nickname, comment))
                logger.info(f"TikTok: comentario recebido de {nickname}")
            except asyncio.QueueFull:
                logger.warning("TikTok: fila cheia; comentario ignorado.")

        @client.on(GiftEvent)
        async def on_gift(event):
            try:
                await self._handle_gift(event)
            except Exception:
                logger.exception("Erro ao processar presente do TikTok.")

        if self.worker_task is None or self.worker_task.done():
            self.worker_task = asyncio.create_task(self._worker())

        try:
            self.listen_task = asyncio.create_task(
                client.start(fetch_live_check=True)
            )
            self.listen_task.add_done_callback(self._listener_finished)
        except Exception as exc:
            self.client = None
            self.listen_task = None
            return {"ok": False, "error": str(exc)}

        return {
            "ok": True,
            "username": username,
            "message": "Conexao solicitada. Consulte /tiktok/status.",
        }

    async def _handle_gift(self, event):
        gift = getattr(event, "gift", None)
        gift_name = str(
            getattr(gift, "name", None)
            or getattr(event, "gift_name", "")
            or ""
        ).strip()

        if not gift_name:
            logger.warning("TikTok: presente recebido sem nome identificavel.")
            return

        is_streakable = bool(getattr(gift, "streakable", False))
        is_streaking = bool(
            getattr(event, "streaking", False)
            or getattr(gift, "streaking", False)
        )

        if is_streakable and is_streaking:
            return

        normalized_name = " ".join(gift_name.casefold().split())
        motion = self.GIFT_MOTIONS.get(normalized_name)

        if motion is None:
            logger.info(
                f"TikTok: presente sem movimento configurado: {gift_name}"
            )
            return

        user = getattr(event, "user", None)
        nickname = (
            getattr(user, "nickname", None)
            or getattr(user, "unique_id", None)
            or "Espectador"
        )

        await self._send_motion_to_browser(motion, gift_name, str(nickname))

    async def _send_motion_to_browser(self, motion, gift_name, nickname):
        selected = None

        for uid, websocket in reversed(
            list(self.ws_handler.client_connections.items())
        ):
            context = self.ws_handler.client_contexts.get(uid)
            if context is not None:
                selected = (uid, websocket)
                break

        if selected is None:
            logger.warning(
                "Presente recebido, mas nenhum navegador da VTuber esta conectado."
            )
            return

        uid, websocket = selected

        # Formato confirmado no frontend: case "play-motion".
        message = {
            "type": "play-motion",
            "group": motion,
            "index": 0,
            "priority": 3,
        }

        try:
            await websocket.send_text(
                json.dumps(message, ensure_ascii=False)
            )
            logger.info(
                f"TikTok: {gift_name} de {nickname} -> "
                f"movimento {motion} (navegador {uid})"
            )
        except Exception:
            logger.exception(
                "Nao foi possivel enviar o movimento ao navegador."
            )

    def _listener_finished(self, task):
        try:
            error = task.exception()
            if error:
                logger.error(f"TikTok listener encerrou com erro: {error}")
            else:
                logger.warning("TikTok listener encerrou.")
        except asyncio.CancelledError:
            pass

    async def disconnect(self):
        client = self.client
        task = self.listen_task
        self.client = None
        self.listen_task = None
        self.username = None

        if client is not None:
            for method_name in ("disconnect", "close"):
                method = getattr(client, method_name, None)
                if method is None:
                    continue
                try:
                    result = method()
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    pass

        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass

        return {"ok": True, "message": "Desconectado do TikTok."}

    def status(self):
        task = self.listen_task
        return {
            "connected": (
                self.client is not None
                and task is not None
                and not task.done()
            ),
            "username": self.username,
            "queued_comments": self.queue.qsize(),
            "browser_clients": len(self.ws_handler.client_connections),
        }

    async def _worker(self):
        while True:
            nickname, comment = await self.queue.get()
            try:
                await self._answer_comment(nickname, comment)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Erro ao responder comentario do TikTok.")
            finally:
                self.queue.task_done()

    async def _answer_comment(self, nickname, comment):
        selected = None

        for uid, websocket in reversed(
            list(self.ws_handler.client_connections.items())
        ):
            context = self.ws_handler.client_contexts.get(uid)
            if context is not None:
                selected = (uid, websocket, context)
                break

        if selected is None:
            logger.warning(
                "Comentario recebido, mas nenhum navegador da VTuber esta conectado."
            )
            return

        uid, websocket, context = selected
        tasks = self.ws_handler.current_conversation_tasks

        previous = tasks.get(uid)
        if previous is not None and not previous.done():
            try:
                await previous
            except Exception:
                pass

        if (
            uid not in self.ws_handler.client_connections
            or uid not in self.ws_handler.client_contexts
        ):
            logger.warning("Sessao do navegador encerrou antes da resposta.")
            return

        context = self.ws_handler.client_contexts[uid]
        websocket = self.ws_handler.client_connections[uid]

        text = (
            f"Comentario de {nickname} na LIVE do TikTok: {comment}\n"
            "Responda diretamente ao comentario, em portugues brasileiro, "
            "de forma natural e curta, como Mili. Nao invente informacoes."
        )

        task = asyncio.create_task(
            process_single_conversation(
                context=context,
                websocket_send=websocket.send_text,
                client_uid=uid,
                user_input=text,
                images=None,
                session_emoji="💬",
                metadata={"skip_history": False},
            )
        )
        tasks[uid] = task

        try:
            await task
        finally:
            if tasks.get(uid) is task:
                tasks.pop(uid, None)