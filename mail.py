from __future__ import annotations

import asyncio
import html
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

import aiohttp
import discord
from discord.ext import tasks
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


DISCORD_TOKEN = os.getenv(
    "DISCORD_TOKEN",
    "MTQ3OTYxNjk3OTMyMDE3NjY2Mg.GYfh7d.U3Hh81DDx2ZQXD63CaLA_luGh1lzVEsax8ruGk",
)
TG_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    "8520726896:AAEP3r9lcf1zVoLolsYuTrfA5L84XpU_zMo",
)
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-1003788415999")
TARGET_CHANNELS = [
    channel_id.strip()
    for channel_id in os.getenv(
        "DISCORD_CHANNEL_IDS", "1472190037230616688,1472182119458537512"
    ).split(",")
    if channel_id.strip()
]
TARGET_CHANNEL_SET = set(TARGET_CHANNELS)

ADMIN_ROLES = {
    "1056881020336607261",
    "1056881271114039326",
    "1230143787238424596",
    "1056884267877150780",
    "1166853829250256957",
    "1056882218485690468",
    "1056887148265095228",
}

KYIV_TZ = ZoneInfo("Europe/Kyiv")
PLAYER_REJOIN_GRACE_SECONDS = _env_int("PLAYER_REJOIN_GRACE_SECONDS", 180)
EVENT_DEDUPE_SECONDS = float(os.getenv("EVENT_DEDUPE_SECONDS", "2.0"))
# Мінімальний інтервал між редагуванням повідомлень Telegram (захист від rate-limit)
TG_EDIT_COOLDOWN_SECONDS = float(os.getenv("TG_EDIT_COOLDOWN_SECONDS", "1.5"))
# Робочі години (Київський час)
WORK_HOUR_START = _env_int("WORK_HOUR_START", 10)  # 10:00
WORK_HOUR_END = _env_int("WORK_HOUR_END", 21)       # до 21:00

# ──────────────────────────────────────────────────────────────
# Логування
# ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
LOGGER = logging.getLogger("voice-monitor")

# ──────────────────────────────────────────────────────────────
# Data models
# ──────────────────────────────────────────────────────────────

StatusType = Literal["waiting", "in_progress", "completed", "abandoned"]


@dataclass
class PlayerState:
    """Tracks a single player within a voice session."""

    tag: str
    first_join_at: datetime
    current_join_at: datetime | None = None
    last_leave_at: datetime | None = None
    pending_until: datetime | None = None
    total_online_seconds: int = 0
    reconnects: int = 0
    disconnects: int = 0


@dataclass
class AdminVisit:
    """One continuous visit of an admin inside the voice channel."""

    tag: str
    join_at: datetime
    leave_at: datetime | None = None


@dataclass
class VoiceSession:
    """Represents a support session in one voice channel."""

    channel_id: str
    channel_name: str
    opened_at: datetime
    players: dict[int, PlayerState] = field(default_factory=dict)
    admins: dict[int, list[AdminVisit]] = field(default_factory=dict)
    status: StatusType = "waiting"
    message_id: int | None = None
    last_rendered_text: str = ""
    last_edit_time: float = 0.0
    events_count: int = 0
    closed_reason: str | None = None


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────


def kyiv_now() -> datetime:
    return datetime.now(KYIV_TZ)


def format_duration(seconds_total: int) -> str:
    seconds_total = max(0, int(seconds_total))
    hours, rem = divmod(seconds_total, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours > 0:
        return f"{hours}г {minutes:02d}хв"
    if minutes > 0:
        return f"{minutes}хв {seconds:02d}с"
    return f"{seconds}с"


def format_clock(moment: datetime) -> str:
    return moment.strftime("%H:%M:%S")


def format_date_clock(moment: datetime) -> str:
    return moment.strftime("%d.%m %H:%M:%S")


def player_online_seconds(player: PlayerState, now: datetime) -> int:
    total = player.total_online_seconds
    if player.current_join_at is not None:
        total += max(0, int((now - player.current_join_at).total_seconds()))
    return total


def player_is_active(player: PlayerState) -> bool:
    return player.current_join_at is not None


def player_is_pending(player: PlayerState, now: datetime) -> bool:
    return (
        player.current_join_at is None
        and player.pending_until is not None
        and now < player.pending_until
    )


# ──────────────────────────────────────────────────────────────
# Bot client
# ──────────────────────────────────────────────────────────────


class VoiceMonitorClient(discord.Client):
    def __init__(self, tg_token: str, tg_chat_id: str, **kwargs):
        super().__init__(**kwargs)

        self._vm_tg_token = tg_token
        self._vm_tg_chat_id = tg_chat_id
        self._vm_http_session: aiohttp.ClientSession | None = None

        # channel_id -> VoiceSession
        self._vm_voice_sessions: dict[str, VoiceSession] = {}
        # channel_id -> set of admin member IDs currently in the channel
        self._vm_active_admins_by_channel: dict[str, set[int]] = {}
        # Deduplication of rapid voice-state events
        self._vm_last_transition_ts: dict[tuple[int, str, str], float] = {}

        self._vm_startup_message_sent = False
        self._vm_last_hour: int = kyiv_now().hour
        # Блокування на канал для захисту від race conditions
        self._vm_channel_locks: dict[str, asyncio.Lock] = {}
        # Задачі публікації (debounce швидких подій)
        self._vm_pending_publishes: dict[str, asyncio.Task] = {}

    # ── lifecycle ────────────────────────────────────────────

    async def setup_hook(self) -> None:
        if self._vm_tg_token and self._vm_tg_chat_id:
            self._vm_http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20)
            )
        self.maintenance_loop.start()

    async def close(self) -> None:
        if self.maintenance_loop.is_running():
            self.maintenance_loop.cancel()
        # Cancel any pending publish tasks
        for task in self._vm_pending_publishes.values():
            task.cancel()
        self._vm_pending_publishes.clear()
        if self._vm_http_session and not self._vm_http_session.closed:
            await self._vm_http_session.close()
        await super().close()

    # ── channel lock helper ──────────────────────────────────

    def _channel_lock(self, channel_id: str) -> asyncio.Lock:
        if channel_id not in self._vm_channel_locks:
            self._vm_channel_locks[channel_id] = asyncio.Lock()
        return self._vm_channel_locks[channel_id]

    # ── role helpers ─────────────────────────────────────────

    def is_within_work_hours(self, moment: datetime | None = None) -> bool:
        check = moment or kyiv_now()
        return WORK_HOUR_START <= check.hour < WORK_HOUR_END

    def is_admin_member(self, member: discord.Member | None) -> bool:
        if member is None:
            return False
        return any(str(role.id) in ADMIN_ROLES for role in getattr(member, "roles", []))

    def _target_channel(self, channel_id: str | None) -> bool:
        return bool(channel_id and channel_id in TARGET_CHANNEL_SET)

    # ── Telegram transport ───────────────────────────────────

    async def _telegram_request(self, method: str, payload: dict[str, str]) -> dict | None:
        if not self._vm_http_session or not self._vm_tg_token:
            return None
        url = f"https://api.telegram.org/bot{self._vm_tg_token}/{method}"
        try:
            async with self._vm_http_session.post(url, data=payload) as response:
                data = await response.json(content_type=None)
                if response.status != 200 or not data.get("ok"):
                    description = str(data.get("description", "Unknown Telegram error"))
                    if method == "editMessageText" and "message is not modified" in description:
                        return {"ok": True, "result": "not_modified"}
                    # Rate-limited by Telegram — back off
                    if "Too Many Requests" in description or response.status == 429:
                        retry_after = data.get("parameters", {}).get("retry_after", 5)
                        LOGGER.warning("TG rate-limited, retry after %ss", retry_after)
                        await asyncio.sleep(int(retry_after) + 1)
                        return await self._telegram_request(method, payload)
                    LOGGER.error("TG %s Error: %s", method, data)
                    return None
                return data
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            LOGGER.error("TG %s Exception: %s", method, err)
            return None

    async def send_telegram_message(self, text: str) -> int | None:
        if not self._vm_tg_chat_id:
            return None
        data = await self._telegram_request(
            "sendMessage",
            {
                "chat_id": self._vm_tg_chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            },
        )
        if not data:
            return None
        return data.get("result", {}).get("message_id")

    async def edit_telegram_message(self, message_id: int, text: str) -> bool:
        if not self._vm_tg_chat_id:
            return False
        data = await self._telegram_request(
            "editMessageText",
            {
                "chat_id": self._vm_tg_chat_id,
                "message_id": str(message_id),
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            },
        )
        return bool(data)

    # ── dedup ────────────────────────────────────────────────

    def _transition_is_duplicate(
        self,
        member_id: int,
        from_channel_id: str | None,
        to_channel_id: str | None,
    ) -> bool:
        now_mono = time.monotonic()
        key = (member_id, from_channel_id or "-", to_channel_id or "-")
        previous = self._vm_last_transition_ts.get(key)
        self._vm_last_transition_ts[key] = now_mono

        # Periodic cleanup
        if len(self._vm_last_transition_ts) > 2000:
            cutoff = now_mono - 30.0
            self._vm_last_transition_ts = {
                k: v for k, v in self._vm_last_transition_ts.items() if v >= cutoff
            }

        return previous is not None and (now_mono - previous) < EVENT_DEDUPE_SECONDS

    # ── display name helpers ─────────────────────────────────

    def _channel_display_name(self, channel_id: str, fallback: str | None = None) -> str:
        try:
            channel = self.get_channel(int(channel_id))
            if channel is not None and hasattr(channel, "name"):
                return str(channel.name)
        except (ValueError, TypeError):
            pass
        return fallback or channel_id

    def _member_display_name(self, channel_id: str, member_id: int, fallback: str | None = None) -> str:
        try:
            channel = self.get_channel(int(channel_id))
            if channel is not None and hasattr(channel, "members"):
                for member in channel.members:
                    if member.id == member_id:
                        return member.display_name
        except (ValueError, TypeError):
            pass
        return fallback or f"ID:{member_id}"

    # ── session management ───────────────────────────────────

    def _get_or_create_session(
        self,
        channel_id: str,
        channel_name: str,
        now: datetime,
    ) -> VoiceSession:
        session = self._vm_voice_sessions.get(channel_id)
        if session is None:
            session = VoiceSession(
                channel_id=channel_id,
                channel_name=channel_name,
                opened_at=now,
            )
            self._vm_voice_sessions[channel_id] = session
            LOGGER.info("New session created for channel %s (%s)", channel_name, channel_id)
        else:
            session.channel_name = channel_name
        return session

    def _session_has_admin_ever(self, session: VoiceSession) -> bool:
        return any(history for history in session.admins.values())

    def _session_has_active_players(self, session: VoiceSession) -> bool:
        """At least one player is currently connected to voice."""
        return any(player_is_active(p) for p in session.players.values())

    def _session_has_pending_players(self, session: VoiceSession, now: datetime) -> bool:
        """At least one player is in grace period (disconnected but might rejoin)."""
        return any(player_is_pending(p, now) for p in session.players.values())

    def _channel_has_active_admin(self, channel_id: str) -> bool:
        return len(self._vm_active_admins_by_channel.get(channel_id, set())) > 0

    def _active_admin_count(self, channel_id: str) -> int:
        return len(self._vm_active_admins_by_channel.get(channel_id, set()))

    # ── player state mutations ───────────────────────────────

    def _upsert_player_join(
        self,
        session: VoiceSession,
        player_id: int,
        tag: str,
        now: datetime,
    ) -> bool:
        player = session.players.get(player_id)
        if player is None:
            session.players[player_id] = PlayerState(
                tag=tag,
                first_join_at=now,
                current_join_at=now,
            )
            session.events_count += 1
            return True

        # Update display name in case it changed
        player.tag = tag

        if player.current_join_at is not None:
            # Already in channel — no-op
            return False

        player.reconnects += 1
        player.current_join_at = now
        player.last_leave_at = None
        player.pending_until = None
        session.events_count += 1
        return True

    def _mark_player_leave(
        self,
        session: VoiceSession,
        player_id: int,
        now: datetime,
        allow_grace: bool = True,
    ) -> bool:
        player = session.players.get(player_id)
        if player is None or player.current_join_at is None:
            return False

        played_now = max(0, int((now - player.current_join_at).total_seconds()))
        player.total_online_seconds += played_now
        player.current_join_at = None
        player.last_leave_at = now
        player.pending_until = (
            now + timedelta(seconds=PLAYER_REJOIN_GRACE_SECONDS) if allow_grace else None
        )
        player.disconnects += 1
        session.events_count += 1
        return True

    def _finalize_expired_pending_players(self, session: VoiceSession, now: datetime) -> bool:
        changed = False
        for player in session.players.values():
            if (
                player.current_join_at is None
                and player.pending_until is not None
                and now >= player.pending_until
            ):
                player.pending_until = None
                changed = True
                session.events_count += 1
        return changed

    # ── admin state mutations ────────────────────────────────

    def _ensure_admin_visit_open(
        self,
        session: VoiceSession,
        admin_id: int,
        tag: str,
        now: datetime,
    ) -> bool:
        history = session.admins.get(admin_id)
        if history is None:
            session.admins[admin_id] = [AdminVisit(tag=tag, join_at=now)]
            session.events_count += 1
            return True

        if history and history[-1].leave_at is None:
            # Already open — just update the display name
            history[-1].tag = tag
            return False

        history.append(AdminVisit(tag=tag, join_at=now))
        session.events_count += 1
        return True

    def _close_admin_visit(
        self,
        session: VoiceSession,
        admin_id: int,
        now: datetime,
    ) -> bool:
        history = session.admins.get(admin_id)
        if not history:
            return False
        if history[-1].leave_at is not None:
            return False
        history[-1].leave_at = now
        session.events_count += 1
        return True

    def _sync_active_admins_to_session(
        self, session: VoiceSession, channel_id: str, now: datetime
    ) -> bool:
        changed = False
        for admin_id in self._vm_active_admins_by_channel.get(channel_id, set()):
            tag = self._member_display_name(channel_id, admin_id)
            if self._ensure_admin_visit_open(session, admin_id, tag, now):
                changed = True
        return changed

    # ── status calculation ───────────────────────────────────

    def _clear_all_pending(self, session: VoiceSession) -> None:
        """Remove grace from all pending players (session is closing)."""
        for player in session.players.values():
            if player.pending_until is not None:
                player.pending_until = None

    def _recalculate_session_status(
        self,
        session: VoiceSession,
        channel_id: str,
        now: datetime,
    ) -> bool:
        previous = session.status
        has_active_players = self._session_has_active_players(session)
        has_pending_players = self._session_has_pending_players(session, now)
        has_active_admin = self._channel_has_active_admin(channel_id)
        admin_ever = self._session_has_admin_ever(session)

        if has_active_players:
            # Players are in voice right now
            session.status = "in_progress" if has_active_admin else "waiting"
        elif admin_ever:
            # All players left but admin was here → request handled → close
            session.status = "completed"
            self._clear_all_pending(session)
        elif has_pending_players:
            # No admin ever came, players might reconnect → keep waiting
            session.status = "waiting"
        else:
            # Nobody left, no admin ever came
            session.status = "abandoned"

        return previous != session.status

    # ── message rendering ────────────────────────────────────

    def _status_emoji(self, status: StatusType) -> str:
        return {
            "waiting": "🟡",
            "in_progress": "🟢",
            "completed": "✅",
            "abandoned": "🔴",
        }.get(status, "❓")

    def _status_label(self, status: StatusType) -> str:
        return {
            "waiting": "Очікування адміна",
            "in_progress": "В роботі",
            "completed": "Закрито",
            "abandoned": "Без відповіді",
        }.get(status, "Невідомо")

    def _build_session_message(self, session: VoiceSession, now: datetime) -> str:
        active_players = sum(1 for p in session.players.values() if player_is_active(p))
        pending_players = sum(
            1 for p in session.players.values()
            if player_is_pending(p, now) and not player_is_active(p)
        )
        offline_players = len(session.players) - active_players - pending_players
        total_players = len(session.players)

        active_admin_ids = self._vm_active_admins_by_channel.get(session.channel_id, set())
        active_admin_count = len(active_admin_ids)
        total_admin_count = len(session.admins)

        status_emoji = self._status_emoji(session.status)
        status_label = self._status_label(session.status)
        session_age = format_duration(int((now - session.opened_at).total_seconds()))

        # ── Header ──
        lines: list[str] = [
            f"{status_emoji} <b>{html.escape(session.channel_name)}</b> │ {status_label}",
            f"<code>────────────────────────────</code>",
            f"🕒 {format_date_clock(now)} (Kyiv)  ·  ⏱ {session_age}",
            "",
        ]

        # ── Players section ──
        if total_players > 0:
            lines.append(
                f"👥 <b>Гравці</b>  "
                f"<code>{active_players}</code> онлайн  ·  "
                f"<code>{pending_players}</code> очікують  ·  "
                f"<code>{offline_players}</code> вийшли"
            )

            sorted_players = sorted(
                session.players.values(),
                key=lambda p: (
                    0 if player_is_active(p) else 1 if player_is_pending(p, now) else 2,
                    p.tag.lower(),
                ),
            )
            for player in sorted_players:
                safe_tag = html.escape(player.tag)
                online_total = format_duration(player_online_seconds(player, now))

                if player_is_active(player):
                    current_live = format_duration(
                        int((now - player.current_join_at).total_seconds())
                    )
                    reconn = f"  ·  🔄 {player.reconnects}" if player.reconnects > 0 else ""
                    lines.append(
                        f"  🟢 <b>{safe_tag}</b>\n"
                        f"      зараз <code>{current_live}</code>  ·  всього <code>{online_total}</code>{reconn}"
                    )
                elif player_is_pending(player, now):
                    away_for = format_duration(
                        int((now - (player.last_leave_at or now)).total_seconds())
                    )
                    grace_remaining = max(
                        0,
                        int(((player.pending_until or now) - now).total_seconds()),
                    )
                    lines.append(
                        f"  🟡 <b>{safe_tag}</b>\n"
                        f"      вийшов <code>{away_for}</code> тому  ·  "
                        f"grace <code>{format_duration(grace_remaining)}</code>"
                    )
                else:
                    details = f"всього <code>{online_total}</code>"
                    if player.reconnects > 0:
                        details += f"  ·  🔄 {player.reconnects}"
                    lines.append(f"  ⚪ <b>{safe_tag}</b>  ·  {details}")
        else:
            lines.append("👥 <b>Гравці</b>  —  немає")

        lines.append("")

        # ── Admins section ──
        if total_admin_count > 0:
            lines.append(
                f"🛡 <b>Адміністрація</b>  "
                f"<code>{active_admin_count}</code> онлайн  ·  "
                f"<code>{total_admin_count}</code> всього"
            )

            for admin_id, visits in sorted(
                session.admins.items(),
                key=lambda item: item[1][0].tag.lower() if item[1] else "",
            ):
                if not visits:
                    continue
                tag = html.escape(visits[-1].tag)
                total_seconds = 0
                active_live_seconds = 0
                for visit in visits:
                    end = visit.leave_at or now
                    total_seconds += max(0, int((end - visit.join_at).total_seconds()))
                    if visit.leave_at is None:
                        active_live_seconds += max(
                            0, int((now - visit.join_at).total_seconds())
                        )
                visits_count = len(visits)
                total_text = format_duration(total_seconds)

                if admin_id in active_admin_ids and active_live_seconds > 0:
                    live_text = format_duration(active_live_seconds)
                    lines.append(
                        f"  🟢 <b>{tag}</b>\n"
                        f"      зараз <code>{live_text}</code>  ·  всього <code>{total_text}</code>  ·  заходів <code>{visits_count}</code>"
                    )
                else:
                    lines.append(
                        f"  ⚪ <b>{tag}</b>\n"
                        f"      офлайн  ·  всього <code>{total_text}</code>  ·  заходів <code>{visits_count}</code>"
                    )
        else:
            lines.append("🛡 <b>Адміністрація</b>  —  ще не заходили")

        # ── Footer ──
        lines.append("")
        lines.append(f"📊 Подій: <code>{session.events_count}</code>")

        if session.closed_reason:
            lines.append(f"📝 <i>{html.escape(session.closed_reason)}</i>")

        return "\n".join(lines)

    # ── publish / debounce ───────────────────────────────────

    async def _publish_session(self, channel_id: str, now: datetime) -> None:
        session = self._vm_voice_sessions.get(channel_id)
        if not session:
            return
        text = self._build_session_message(session, now)
        if text == session.last_rendered_text:
            return

        if session.message_id is None:
            message_id = await self.send_telegram_message(text)
            if message_id:
                session.message_id = message_id
                session.last_rendered_text = text
                session.last_edit_time = time.monotonic()
        else:
            # Respect edit cooldown to avoid Telegram rate-limits
            elapsed = time.monotonic() - session.last_edit_time
            if elapsed < TG_EDIT_COOLDOWN_SECONDS:
                await asyncio.sleep(TG_EDIT_COOLDOWN_SECONDS - elapsed)

            if await self.edit_telegram_message(session.message_id, text):
                session.last_rendered_text = text
                session.last_edit_time = time.monotonic()

    async def _schedule_publish(self, channel_id: str) -> None:
        """Debounced publish — waits a short time to batch rapid events."""
        # Cancel any already-pending publish for this channel
        existing = self._vm_pending_publishes.pop(channel_id, None)
        if existing and not existing.done():
            existing.cancel()

        async def _deferred():
            await asyncio.sleep(0.3)  # batch window
            self._vm_pending_publishes.pop(channel_id, None)
            now = kyiv_now()
            await self._publish_session(channel_id, now)

        task = asyncio.create_task(_deferred())
        self._vm_pending_publishes[channel_id] = task

    # ── session finalization ─────────────────────────────────

    async def _finalize_session_if_needed(self, channel_id: str, now: datetime) -> bool:
        session = self._vm_voice_sessions.get(channel_id)
        if not session:
            return False

        self._finalize_expired_pending_players(session, now)
        self._recalculate_session_status(session, channel_id, now)

        if session.status in ("completed", "abandoned"):
            # Session is over — send final update and remove
            await self._publish_session(channel_id, now)
            self._vm_voice_sessions.pop(channel_id, None)
            LOGGER.info(
                "Session closed for channel %s (%s) — %s",
                session.channel_name,
                channel_id,
                session.status,
            )
            return True

        # Session still active or waiting
        await self._publish_session(channel_id, now)
        return False

    # ── startup sync ─────────────────────────────────────────

    async def _sync_from_current_voice_state(self, now: datetime) -> None:
        """Scan all target voice channels and reconstruct sessions from current members."""
        for channel_id in TARGET_CHANNELS:
            try:
                channel = self.get_channel(int(channel_id))
            except (ValueError, TypeError):
                continue
            if channel is None or not hasattr(channel, "members"):
                continue

            admins: set[int] = set()
            players: list[discord.Member] = []
            for member in channel.members:
                if self.user and member.id == self.user.id:
                    continue
                if self.is_admin_member(member):
                    admins.add(member.id)
                else:
                    players.append(member)
            self._vm_active_admins_by_channel[channel_id] = admins

            if not players:
                continue

            session = self._get_or_create_session(channel_id, str(channel.name), now)
            for player in players:
                self._upsert_player_join(session, player.id, player.display_name, now)
            self._sync_active_admins_to_session(session, channel_id, now)
            self._recalculate_session_status(session, channel_id, now)
            if session.closed_reason is None:
                session.closed_reason = "Сесію відновлено після перезапуску бота"
            await self._publish_session(channel_id, now)

    # ── Discord events ───────────────────────────────────────

    async def on_ready(self) -> None:
        LOGGER.info("Підключено до Discord як %s", self.user)

        now = kyiv_now()
        self._vm_last_hour = now.hour
        await self._sync_from_current_voice_state(now)

        if not self._vm_startup_message_sent:
            channels_view = ", ".join(
                f"<code>{self._channel_display_name(cid, cid)}</code>"
                for cid in TARGET_CHANNELS
            )
            status = (
                "🟢 Активний (робочий час)"
                if self.is_within_work_hours(now)
                else "🌙 Пауза (поза робочим часом)"
            )
            await self.send_telegram_message(
                f"🤖 <b>Бот запущено</b>\n"
                f"<code>────────────────────────────</code>\n"
                f"🕒 {format_date_clock(now)} (Київ)\n"
                f"📌 Статус: {status}\n"
                f"🕙 Графік: <code>{WORK_HOUR_START:02d}:00 – {WORK_HOUR_END:02d}:00</code>\n"
                f"♻️ Grace: <code>{PLAYER_REJOIN_GRACE_SECONDS}с</code>\n"
                f"🎯 Канали: {channels_view}"
            )
            self._vm_startup_message_sent = True

    async def on_disconnect(self) -> None:
        LOGGER.warning("Від'єднано від Discord — очікуємо перепідключення…")

    async def on_resumed(self) -> None:
        LOGGER.info("Discord сесію відновлено — синхронізуємо стан войсів")
        now = kyiv_now()
        await self._sync_from_current_voice_state(now)

    # ── voice-state handler ──────────────────────────────────

    async def _handle_player_join(
        self,
        member: discord.Member,
        channel_id: str,
        channel_name: str,
        now: datetime,
    ) -> None:
        session = self._get_or_create_session(channel_id, channel_name, now)
        changed = self._upsert_player_join(session, member.id, member.display_name, now)
        changed = self._sync_active_admins_to_session(session, channel_id, now) or changed
        status_changed = self._recalculate_session_status(session, channel_id, now)
        if changed or status_changed or session.message_id is None:
            session.closed_reason = None
            await self._schedule_publish(channel_id)

    async def _handle_player_leave(
        self,
        member: discord.Member,
        channel_id: str,
        now: datetime,
        allow_grace: bool = True,
    ) -> None:
        session = self._vm_voice_sessions.get(channel_id)
        if not session:
            return
        changed = self._mark_player_leave(session, member.id, now, allow_grace=allow_grace)
        status_changed = self._recalculate_session_status(session, channel_id, now)
        if changed or status_changed:
            if not await self._finalize_session_if_needed(channel_id, now):
                await self._schedule_publish(channel_id)

    async def _handle_admin_join(
        self,
        member: discord.Member,
        channel_id: str,
        now: datetime,
    ) -> None:
        session = self._vm_voice_sessions.get(channel_id)
        if not session:
            return
        # Only open admin visit if there are active players (not just pending)
        if not self._session_has_active_players(session):
            return
        changed = self._ensure_admin_visit_open(session, member.id, member.display_name, now)
        status_changed = self._recalculate_session_status(session, channel_id, now)
        if changed or status_changed:
            await self._schedule_publish(channel_id)

    async def _handle_admin_leave(
        self,
        member: discord.Member,
        channel_id: str,
        now: datetime,
    ) -> None:
        session = self._vm_voice_sessions.get(channel_id)
        if not session:
            return
        changed = self._close_admin_visit(session, member.id, now)
        status_changed = self._recalculate_session_status(session, channel_id, now)
        if changed or status_changed:
            if not await self._finalize_session_if_needed(channel_id, now):
                await self._schedule_publish(channel_id)

    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        # Skip self
        if self.user and member.id == self.user.id:
            return

        left_channel_id = str(before.channel.id) if before.channel else None
        joined_channel_id = str(after.channel.id) if after.channel else None

        # Same channel — mute/deafen/etc., ignore
        if left_channel_id == joined_channel_id:
            return

        left_is_target = self._target_channel(left_channel_id)
        joined_is_target = self._target_channel(joined_channel_id)
        if not left_is_target and not joined_is_target:
            return

        if self._transition_is_duplicate(member.id, left_channel_id, joined_channel_id):
            return

        now = kyiv_now()
        is_admin = self.is_admin_member(member)

        # ── Update raw admin presence (always, even before session logic) ──
        if is_admin:
            if left_is_target and left_channel_id:
                self._vm_active_admins_by_channel.setdefault(left_channel_id, set()).discard(
                    member.id
                )
            if joined_is_target and joined_channel_id:
                self._vm_active_admins_by_channel.setdefault(joined_channel_id, set()).add(
                    member.id
                )

        # ── Вихід зі старого каналу ──
        if left_is_target and left_channel_id:
            async with self._channel_lock(left_channel_id):
                if is_admin:
                    LOGGER.info("[Адмін вийшов] %s <- %s", member.display_name, left_channel_id)
                    if self.is_within_work_hours(now):
                        await self._handle_admin_leave(member, left_channel_id, now)
                else:
                    LOGGER.info("[Гравець вийшов] %s <- %s", member.display_name, left_channel_id)
                    if self.is_within_work_hours(now):
                        await self._handle_player_leave(
                            member,
                            left_channel_id,
                            now,
                            # Без grace при переміщенні між цільовими каналами
                            allow_grace=not joined_is_target,
                        )

        # ── Вхід у новий канал ──
        if joined_is_target and joined_channel_id:
            async with self._channel_lock(joined_channel_id):
                channel_name = self._channel_display_name(joined_channel_id, joined_channel_id)
                if is_admin:
                    LOGGER.info("[Адмін зайшов] %s -> %s", member.display_name, joined_channel_id)
                    if self.is_within_work_hours(now):
                        await self._handle_admin_join(member, joined_channel_id, now)
                else:
                    LOGGER.info("[Гравець зайшов] %s -> %s", member.display_name, joined_channel_id)
                    if self.is_within_work_hours(now):
                        await self._handle_player_join(
                            member, joined_channel_id, channel_name, now
                        )

    # ── maintenance loop ─────────────────────────────────────

    @tasks.loop(seconds=20)
    async def maintenance_loop(self) -> None:
        now = kyiv_now()
        current_hour = now.hour

        # ── Початок зміни ──
        if current_hour == WORK_HOUR_START and self._vm_last_hour == WORK_HOUR_START - 1:
            LOGGER.info("Зміна розпочата о %02d:00", WORK_HOUR_START)
            await self.send_telegram_message(
                f"🚀 <b>Зміна розпочата</b>\n"
                f"<code>────────────────────────────</code>\n"
                f"🕒 {format_date_clock(now)} (Київ)\n"
                f"✅ Моніторинг <b>активовано</b>\n"
                f"🕙 Графік: <code>{WORK_HOUR_START:02d}:00 – {WORK_HOUR_END:02d}:00</code>"
            )
            await self._sync_from_current_voice_state(now)

        # ── Кінець зміни ──
        if current_hour == WORK_HOUR_END and self._vm_last_hour == WORK_HOUR_END - 1:
            LOGGER.info("Зміна завершена о %02d:00", WORK_HOUR_END)
            # Закриваємо всі активні сесії
            for channel_id in list(self._vm_voice_sessions.keys()):
                session = self._vm_voice_sessions.get(channel_id)
                if not session:
                    continue
                for player in session.players.values():
                    if player.current_join_at is not None:
                        played = max(0, int((now - player.current_join_at).total_seconds()))
                        player.total_online_seconds += played
                        player.current_join_at = None
                        player.last_leave_at = now
                        player.pending_until = None
                    elif player.pending_until is not None:
                        player.pending_until = None
                session.closed_reason = f"Автозавершення: кінець зміни о {WORK_HOUR_END:02d}:00"
                self._recalculate_session_status(session, channel_id, now)
                await self._publish_session(channel_id, now)
            self._vm_voice_sessions.clear()
            await self.send_telegram_message(
                f"🌙 <b>Зміна завершена</b>\n"
                f"<code>────────────────────────────</code>\n"
                f"🕒 {format_date_clock(now)} (Київ)\n"
                f"💤 Моніторинг <b>призупинено</b>\n"
                f"⏰ До зустрічі о <code>{WORK_HOUR_START:02d}:00</code>"
            )

        self._vm_last_hour = current_hour

        # ── Обслуговування активних сесій (тільки в робочий час) ──
        if not self.is_within_work_hours(now):
            # Очищаємо locks поза робочим часом
            stale_locks = [
                cid for cid in list(self._vm_channel_locks.keys())
                if cid not in self._vm_voice_sessions
                and not self._vm_channel_locks[cid].locked()
            ]
            for cid in stale_locks:
                self._vm_channel_locks.pop(cid, None)
            return

        for channel_id in list(self._vm_voice_sessions.keys()):
            async with self._channel_lock(channel_id):
                session = self._vm_voice_sessions.get(channel_id)
                if not session:
                    continue
                changed = self._finalize_expired_pending_players(session, now)
                changed = (
                    self._recalculate_session_status(session, channel_id, now) or changed
                )
                if session.status in ("completed", "abandoned"):
                    await self._finalize_session_if_needed(channel_id, now)
                elif changed:
                    await self._publish_session(channel_id, now)

        # Очищення старих locks (захист від витоку пам'яті)
        stale_locks = [
            cid
            for cid in list(self._vm_channel_locks.keys())
            if cid not in self._vm_voice_sessions
            and not self._vm_channel_locks[cid].locked()
        ]
        for cid in stale_locks:
            self._vm_channel_locks.pop(cid, None)

    @maintenance_loop.before_loop
    async def before_maintenance_loop(self) -> None:
        await self.wait_until_ready()


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────


def main() -> None:
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN порожній — перевірте налаштування.")

    client = VoiceMonitorClient(
        tg_token=TG_TOKEN,
        tg_chat_id=TG_CHAT_ID,
    )

    try:
        client.run(DISCORD_TOKEN)
    except discord.LoginFailure as err:
        LOGGER.error("Помилка входу в Discord: %s", err)
    except KeyboardInterrupt:
        LOGGER.info("Бот зупинено користувачем.")


if __name__ == "__main__":
    main()
