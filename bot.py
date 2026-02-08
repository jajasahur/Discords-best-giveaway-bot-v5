import asyncio
import os
import random
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import logging
import json

# configure logger early to suppress known noisy warnings before importing discord
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
# console handler
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)
# file handler (writes to bot.log in the project folder)
fh = logging.FileHandler('bot.log', encoding='utf-8')
fh.setFormatter(formatter)
logger.addHandler(fh)
# silence optional PyNaCl/voice warning from voice client
logging.getLogger('discord.voice_client').setLevel(logging.ERROR)
# add a tiny filter on the discord.client logger to drop only the specific PyNaCl warning
class _PyNaClFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
            if 'PyNaCl is not installed' in msg and 'voice will NOT be supported' in msg:
                return False
        except Exception:
            pass
        return True

logging.getLogger('discord.client').addFilter(_PyNaClFilter())
logging.getLogger('discord').setLevel(logging.INFO)

import discord
from discord import app_commands
from discord.ext import commands

# small set of fun emojis to sprinkle into result messages
EMOJI_POOL = ["🎉", "🏆", "✨", "🥳", "🎊", "🔥", "💥", "🎈", "🪙", "👏"]

# colored circles for Memory game (memorize by color)
MEMORY_COLORS = ["🔴", "🟠", "🟡", "🟢", "🔵", "🟣"]

# credits storage
CREDITS_FILE = "credits.json"

# lightweight trivia bank
TRIVIA_BANK = {
    "easy": [
        {"q": "What color do you get when you mix red and blue?", "a": "purple"},
        {"q": "How many days are in a week?", "a": "7"},
        {"q": "What planet do we live on?", "a": "earth"},
        {"q": "Which animal says 'meow'?", "a": "cat"},
        {"q": "What is 10 + 5?", "a": "15"},
        {"q": "How many hours are in a day?", "a": "24"},
        {"q": "What is 3 + 7?", "a": "10"},
        {"q": "What is the opposite of hot?", "a": "cold"},
        {"q": "What color is the sky on a clear day?", "a": "blue"},
        {"q": "How many letters are in the English alphabet?", "a": "26"},
        {"q": "What shape has three sides?", "a": "triangle"},
        {"q": "Which animal barks?", "a": "dog"},
        {"q": "What is 9 - 4?", "a": "5"},
        {"q": "What do bees make?", "a": "honey"},
        {"q": "What is 6 x 2?", "a": "12"},
        {"q": "What is 5 + 9?", "a": "14"},
        {"q": "What is 8 - 3?", "a": "5"},
        {"q": "What is 4 x 4?", "a": "16"},
        {"q": "What is 20 / 5?", "a": "4"},
        {"q": "Which season comes after spring?", "a": "summer"},
        {"q": "Which season is the coldest?", "a": "winter"},
        {"q": "What do you call a baby cat?", "a": "kitten"},
        {"q": "What color are bananas when ripe?", "a": "yellow"},
        {"q": "How many legs does a spider have?", "a": "8"},
        {"q": "What is the first month of the year?", "a": "january"},
        {"q": "What is the last day of the week (commonly taught)?", "a": "sunday"},
    ],
    "medium": [
        {"q": "What gas do plants absorb from the air?", "a": "carbon dioxide"},
        {"q": "How many continents are there?", "a": "7"},
        {"q": "What is the capital of Canada?", "a": "ottawa"},
        {"q": "Which ocean is the largest?", "a": "pacific"},
        {"q": "What is 12 × 8?", "a": "96"},
        {"q": "What is the capital of Spain?", "a": "madrid"},
        {"q": "Who painted the Mona Lisa?", "a": "leonardo da vinci"},
        {"q": "What is the largest planet in our solar system?", "a": "jupiter"},
        {"q": "What is H2O commonly called?", "a": "water"},
        {"q": "Which instrument has 88 keys?", "a": "piano"},
        {"q": "What is 15% of 200?", "a": "30"},
        {"q": "What is the freezing point of water in Celsius?", "a": "0"},
        {"q": "Which continent is the Sahara Desert on?", "a": "africa"},
        {"q": "How many minutes are in an hour?", "a": "60"},
        {"q": "What is the capital of Japan?", "a": "tokyo"},
        {"q": "What is the largest mammal?", "a": "blue whale"},
        {"q": "Which planet is closest to the sun?", "a": "mercury"},
        {"q": "What is the main ingredient in bread?", "a": "flour"},
        {"q": "How many sides does a hexagon have?", "a": "6"},
        {"q": "What is the capital of Brazil?", "a": "brasilia"},
        {"q": "What is 9 x 11?", "a": "99"},
        {"q": "Who wrote 'Romeo and Juliet'?", "a": "william shakespeare"},
        {"q": "What is the currency of Japan?", "a": "yen"},
        {"q": "What is the largest continent?", "a": "asia"},
        {"q": "Which metal is liquid at room temperature?", "a": "mercury"},
    ],
    "hard": [
        {"q": "What is the chemical symbol for gold?", "a": "au"},
        {"q": "Which planet has the most moons (as commonly taught)?", "a": "saturn"},
        {"q": "What year did the first iPhone release?", "a": "2007"},
        {"q": "What is the square root of 144?", "a": "12"},
        {"q": "What is the capital of Australia?", "a": "canberra"},
        {"q": "What is the longest river in the world (commonly taught)?", "a": "nile"},
        {"q": "Who wrote '1984'?", "a": "george orwell"},
        {"q": "What is the chemical symbol for potassium?", "a": "k"},
        {"q": "Which element has atomic number 8?", "a": "oxygen"},
        {"q": "How many bones are in the adult human body?", "a": "206"},
        {"q": "What year did World War II end?", "a": "1945"},
        {"q": "What is the smallest prime number?", "a": "2"},
        {"q": "What is the capital of New Zealand?", "a": "wellington"},
        {"q": "Which planet is known as the Red Planet?", "a": "mars"},
        {"q": "What is the speed of light in vacuum (km/s, rounded)?", "a": "300000"},
        {"q": "What is the chemical symbol for sodium?", "a": "na"},
        {"q": "Which gas is most abundant in Earth's atmosphere?", "a": "nitrogen"},
        {"q": "What is the capital of Norway?", "a": "oslo"},
        {"q": "What is 17 x 19?", "a": "323"},
        {"q": "Which element has atomic number 26?", "a": "iron"},
        {"q": "What is the smallest planet in our solar system?", "a": "mercury"},
        {"q": "Which composer wrote the 'Moonlight Sonata'?", "a": "beethoven"},
        {"q": "What is the capital of Thailand?", "a": "bangkok"},
        {"q": "Which blood type is the universal donor?", "a": "o negative"},
        {"q": "What is the value of pi to three decimal places?", "a": "3.142"},
    ],
    "nightmare": [
        {"q": "What is the only even prime number?", "a": "2"},
        {"q": "What is the derivative of sin(x)? (one word)", "a": "cos"},
        {"q": "In chess notation, what is the letter for a knight?", "a": "n"},
        {"q": "What is the chemical symbol for tungsten?", "a": "w"},
        {"q": "What is the 12th Fibonacci number? (F1=1, F2=1)", "a": "144"},
        {"q": "What is the capital of Iceland?", "a": "reykjavik"},
        {"q": "What is the SI unit of electric resistance?", "a": "ohm"},
        {"q": "What is 2^10?", "a": "1024"},
        {"q": "What is the base-2 representation of the number 10?", "a": "1010"},
        {"q": "What is the chemical symbol for antimony?", "a": "sb"},
        {"q": "Who wrote 'The Republic'?", "a": "plato"},
        {"q": "What is the largest bone in the human body?", "a": "femur"},
        {"q": "What is the term for a word that reads the same backward?", "a": "palindrome"},
        {"q": "What is the square root of 169?", "a": "13"},
        {"q": "What is the sum of the angles in a triangle (degrees)?", "a": "180"},
    ],
}


def format_prize_with_multiplier(prize: str, multiplier: int) -> str:
    """Return a display string for the prize with multiplier.

    - If the prize looks like a number with optional suffix (k/m/b), compute the multiplied value
      and return a human-friendly string (e.g. "1m" -> "2m" when multiplier=2).
    - If the prize is a plain number, multiply and format with separators.
    - If the prize is non-numeric, prefix with the multiplier (e.g. "pencil" -> "2 pencil").
    """
    if multiplier <= 1:
        return prize
    s = prize.strip()
    # preserve leading currency symbol if present
    prefix = ''
    if s.startswith('$'):
        prefix = '$'
        s_val = s[1:]
    else:
        s_val = s
    s_val = s_val.replace(',', '').lower()

    # match numeric with suffix like 2.5m, 1k, 100
    m = re.match(r'^([0-9]+(?:\.[0-9]+)?)([kmb])?$', s_val)
    if m:
        num = float(m.group(1))
        suf = m.group(2)
        if suf:
            # multiply the numeric part, then normalize suffix up if needed
            total = num * multiplier
            order = ['k', 'm', 'b']
            idx = order.index(suf)
            # normalize total to next suffixes if >=1000
            while total >= 1000 and idx + 1 < len(order):
                total = total / 1000.0
                idx += 1
            new_suf = order[idx]
            # format number
            if float(total).is_integer():
                disp_num = str(int(total))
            else:
                disp_num = ('{:.2f}'.format(total)).rstrip('0').rstrip('.')
            return f"{prefix}{disp_num}{new_suf}"
        else:
            # plain number
            total = num * multiplier
            if float(total).is_integer():
                return f"{prefix}{int(total):,}"
            else:
                return f"{prefix}{total:,.2f}"

    # not numeric: prefix count (e.g. 'pencil' -> '2 pencil')
    return f"{multiplier} {prize}"


def format_prize_divided(prize: str, divisor: int) -> str:
    """Return a display string for the prize divided by divisor.

    Handles numeric values with k/m/b suffixes and plain numbers. For non-numeric
    prizes that start with a count (e.g. "2 pencils"), attempt to split the count.
    Otherwise return a readable note that the prize is split.
    """
    if divisor <= 1:
        return prize
    s = prize.strip()
    prefix = ''
    if s.startswith('$'):
        prefix = '$'
        s_val = s[1:]
    else:
        s_val = s
    s_val = s_val.replace(',', '').lower()

    # numeric with suffix
    m = re.match(r'^([0-9]+(?:\.[0-9]+)?)([kmb])?$', s_val)
    if m:
        num = float(m.group(1))
        suf = m.group(2)
        # convert to raw units (no suffix)
        unit_mul = {'k': 1_000, 'm': 1_000_000, 'b': 1_000_000_000}
        if suf:
            raw = num * unit_mul[suf]
        else:
            raw = num
        half = raw / float(divisor)
        # pick suffix for display
        order = ['', 'k', 'm', 'b']
        idx = 0
        while idx + 1 < len(order) and half >= 1000.0:
            half = half / 1000.0
            idx += 1
        disp_suf = order[idx]
        if float(half).is_integer():
            disp_num = str(int(half))
        else:
            disp_num = ('{:.2f}'.format(half)).rstrip('0').rstrip('.')
        return f"{prefix}{disp_num}{disp_suf}"

    # plain number (no suffix)
    m2 = re.match(r'^([0-9]+(?:\.[0-9]+)?)$', s_val)
    if m2:
        val = float(m2.group(1))
        res = val / float(divisor)
        if float(res).is_integer():
            return f"{prefix}{int(res):,}"
        else:
            return f"{prefix}{res:,.2f}"

    # tries to handle "<count> item" formats like "2 pencils"
    m3 = re.match(r'^(\d+)\s+(.*)$', s)
    if m3:
        cnt = int(m3.group(1))
        item = m3.group(2)
        per = cnt / float(divisor)
        if per.is_integer():
            return f"{int(per)} {item}"
        else:
            return f"{per:.2f} {item}"

    # fallback
    return f"(split) {prize}"

DATA_LOCK = asyncio.Lock()

def parse_duration(s: str) -> int:
    s = s.strip().lower()
    if not s:
        raise ValueError("Empty duration")
    # find all number+unit pairs, e.g. '1 day', '2h', '30 minutes'
    parts = re.findall(r"(\d+)\s*(y|years?|mo|months?|w|weeks?|d|days?|h|hours?|m|mins?|minutes?|s|secs?|seconds?)\b", s)
    if not parts:
        # fallback: maybe a single number (seconds)
        m = re.match(r"^(\d+)$", s)
        if m:
            val = int(m.group(1))
            if val <= 0:
                raise ValueError("Duration must be positive")
            return val
        raise ValueError("Invalid duration format")

    total = 0
    for amount, unit in parts:
        val = int(amount)
        if unit.startswith('y'):
            total += val * 31536000
        elif unit.startswith('mo') or unit.startswith('month'):
            total += val * 2592000
        elif unit.startswith('w'):
            total += val * 604800
        elif unit.startswith('d'):
            total += val * 86400
        elif unit.startswith('h'):
            total += val * 3600
        elif unit in ('m', 'min', 'mins', 'minute', 'minutes') or unit == 'm':
            total += val * 60
        elif unit.startswith('s'):
            total += val
        else:
            raise ValueError(f"Unknown time unit: {unit}")

    if total <= 0:
        raise ValueError("Duration resolved to 0 or negative")
    return total


def parse_bid_amount(s: str) -> int:
    """Parse a bid string like '1k', '1m', '2,500', '100' into an integer amount (units are arbitrary).
    Supports suffixes: k (thousand), m (million), b (billion). Returns integer amount or raises ValueError.
    """
    if not s:
        raise ValueError("Empty bid")
    s = s.strip().lower().replace(',', '')
    m = re.match(r'^([0-9]+(?:\.[0-9]+)?)([kmb])?$', s)
    if not m:
        # try integer parse
        try:
            return int(float(s))
        except Exception:
            raise ValueError("Invalid bid format")
    val = float(m.group(1))
    suf = m.group(2)
    mul = 1
    if suf == 'k':
        mul = 1_000
    elif suf == 'm':
        mul = 1_000_000
    elif suf == 'b':
        mul = 1_000_000_000
    amt = int(val * mul)
    if amt <= 0:
        raise ValueError("Bid must be positive")
    return amt


def resolve_role_from_input(role_input: str, guild: Optional[discord.Guild]):
    if not role_input:
        return None, None
    if not guild:
        return None, "This server role requirement can't be set outside a server."
    role = None
    m = re.search(r'(\d{5,})', role_input)
    if m:
        try:
            role = guild.get_role(int(m.group(1)))
        except Exception:
            role = None
    if role is None:
        for r in guild.roles:
            if r.name.lower() == role_input.lower():
                role = r
                break
    if role is None:
        return None, "Role not found. Use a role mention, ID, or exact name."
    return role, None


def load_credits() -> dict:
    try:
        if not os.path.exists(CREDITS_FILE):
            return {}
        with open(CREDITS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # ensure int values
        return {str(k): int(v) for k, v in data.items()}
    except Exception:
        return {}


def save_credits(credits: dict) -> None:
    try:
        with open(CREDITS_FILE, 'w', encoding='utf-8') as f:
            json.dump(credits, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def prize_to_credits(prize: str) -> int:
    if not prize:
        return 0
    s = prize.strip().lower().replace(',', '')
    if s.startswith('$'):
        s = s[1:]
    # extract first number+suffix occurrence
    m = re.search(r'([0-9]+(?:\.[0-9]+)?)([kmb])\b', s)
    if not m:
        return 0
    num = float(m.group(1))
    suf = m.group(2)
    if suf == 'm':
        return int(num)
    if suf == 'b':
        return int(num * 1000)
    # k or anything below 1m yields 0 credits
    return 0


async def award_credits_for_prize(botref, user_ids, prize: str, multiplier: int = 1, split: int = 1):
    if not user_ids:
        return
    base = prize_to_credits(prize)
    if base <= 0:
        return
    total = int(base * multiplier)
    per_user = int(total / max(1, split))
    if per_user <= 0:
        return
    async with DATA_LOCK:
        credits = botref.credits
        for uid in user_ids:
            key = str(uid)
            credits[key] = int(credits.get(key, 0)) + per_user
        botref.credits = credits
        save_credits(botref.credits)


class JoinView(discord.ui.View):
    def __init__(self, gid: str, bot_ref):
        super().__init__(timeout=None)
        self.gid = gid
        self.bot = bot_ref
        join_btn = discord.ui.Button(label="Join", style=discord.ButtonStyle.success)

        async def join_cb(interaction: discord.Interaction):
            req_role_id = None
            async with DATA_LOCK:
                game = None
                for d in (self.bot.active_giveaways, self.bot.active_sos, self.bot.active_dbd, self.bot.active_rps, self.bot.active_memory, self.bot.active_maze, self.bot.active_reactroulette, self.bot.active_trivia, self.bot.active_don):
                    g = d.get(self.gid)
                    if g:
                        game = g
                        break
                if not game:
                    await interaction.response.send_message("This game is no longer active.", ephemeral=True)
                    return
                req_role_id = game.get('allowed_role_id')
                entries = game.get('entries', set())
                if interaction.user.id in entries:
                    await interaction.response.send_message("You've already joined.", ephemeral=True)
                    return

            if req_role_id:
                if not interaction.guild:
                    await interaction.response.send_message("This game requires a role, but this interaction isn't in a server.", ephemeral=True)
                    return
                role = interaction.guild.get_role(req_role_id)
                if not role:
                    await interaction.response.send_message("The required role no longer exists.", ephemeral=True)
                    return
                member = interaction.user
                if not isinstance(member, discord.Member):
                    try:
                        member = await interaction.guild.fetch_member(interaction.user.id)
                    except Exception:
                        member = None
                if not member or role not in getattr(member, "roles", []):
                    await interaction.response.send_message(f"Only members with the {role.mention} role can join.", ephemeral=True)
                    return

            async with DATA_LOCK:
                game = None
                for d in (self.bot.active_giveaways, self.bot.active_sos, self.bot.active_dbd, self.bot.active_rps, self.bot.active_memory, self.bot.active_maze, self.bot.active_reactroulette, self.bot.active_trivia, self.bot.active_don):
                    g = d.get(self.gid)
                    if g:
                        game = g
                        break
                if not game:
                    await interaction.response.send_message("This game is no longer active.", ephemeral=True)
                    return
                entries = game.get('entries', set())
                if interaction.user.id in entries:
                    await interaction.response.send_message("You've already joined.", ephemeral=True)
                    return
                entries.add(interaction.user.id)
                game['entries'] = entries
            # try to update the original message's embed entries field
            try:
                msg = None
                if game.get('channel_id'):
                    ch = self.bot.get_channel(game.get('channel_id'))
                    if ch:
                        msg = await ch.fetch_message(game.get('message_id'))
                if msg and msg.embeds:
                    old = msg.embeds[0]
                    # clone the embed to preserve fields, footer, thumbnail, etc.
                    new = discord.Embed.from_dict(old.to_dict())
                    # get the latest entries count from current game state to avoid stale counts
                    try:
                        latest_count = None
                        async with DATA_LOCK:
                            latest_game = None
                            for d in (self.bot.active_giveaways, self.bot.active_sos, self.bot.active_dbd, self.bot.active_rps, self.bot.active_memory, self.bot.active_maze, self.bot.active_reactroulette, self.bot.active_trivia, self.bot.active_don):
                                g = d.get(self.gid)
                                if g:
                                    latest_game = g
                                    break
                            if latest_game:
                                latest_count = len(latest_game.get('entries', []))
                    except Exception:
                        latest_count = None
                    found_entries = False
                    for idx, f in enumerate(new.fields):
                        if f.name.lower() == 'entries':
                            found_entries = True
                            new.set_field_at(
                                idx,
                                name=f.name,
                                value=str(latest_count if latest_count is not None else len(entries)),
                                inline=f.inline,
                            )
                    if not found_entries:
                        new.add_field(name="Entries", value=str(latest_count if latest_count is not None else len(entries)))
                    try:
                        # re-attach a fresh JoinView so the message keeps a working view
                        await msg.edit(embed=new, view=JoinView(self.gid, self.bot))
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                await interaction.response.send_message("Joined!", ephemeral=True)
            except Exception:
                pass

        join_btn.callback = join_cb
        self.add_item(join_btn)


class LuckyNumberView(discord.ui.View):
    def __init__(self, sid: str, bot_ref):
        super().__init__(timeout=None)
        self.sid = sid
        self.bot = bot_ref
        guess_btn = discord.ui.Button(label="Enter Number", style=discord.ButtonStyle.primary)

        async def guess_cb(interaction: discord.Interaction):
            botref = self.bot
            sid = self.sid
            async with DATA_LOCK:
                game = botref.active_luckynumber.get(sid)
            if not game:
                await interaction.response.send_message("This game is no longer active.", ephemeral=True)
                return
            req_role_id = game.get('allowed_role_id')
            if req_role_id:
                if not interaction.guild:
                    await interaction.response.send_message("This game requires a role, but this interaction isn't in a server.", ephemeral=True)
                    return
                role = interaction.guild.get_role(req_role_id)
                if not role:
                    await interaction.response.send_message("The required role no longer exists.", ephemeral=True)
                    return
                member = interaction.user
                if not isinstance(member, discord.Member):
                    try:
                        member = await interaction.guild.fetch_member(interaction.user.id)
                    except Exception:
                        member = None
                if not member or role not in getattr(member, "roles", []):
                    await interaction.response.send_message(f"Only members with the {role.mention} role can play.", ephemeral=True)
                    return

            class GuessModal(discord.ui.Modal, title="Lucky Number: Guess"):
                guess = discord.ui.TextInput(
                    label="Your number (1-100)",
                    style=discord.TextStyle.short,
                    placeholder="e.g. 42",
                    required=True,
                    max_length=3,
                )

                async def on_submit(self, modal_interaction: discord.Interaction):
                    # validate input
                    try:
                        val = int(self.guess.value.strip())
                        if val < 1 or val > 100:
                            raise ValueError("Out of range")
                    except Exception:
                        await modal_interaction.response.send_message("Please enter a whole number from 1 to 100.", ephemeral=True)
                        return

                    async with DATA_LOCK:
                        game = botref.active_luckynumber.get(sid)
                    if not game:
                        await modal_interaction.response.send_message("This game is no longer active.", ephemeral=True)
                        return

                    # re-check role requirement at submit time
                    req_role_id = game.get('allowed_role_id')
                    if req_role_id:
                        if not modal_interaction.guild:
                            await modal_interaction.response.send_message("This game requires a role, but this interaction isn't in a server.", ephemeral=True)
                            return
                        role = modal_interaction.guild.get_role(req_role_id)
                        if not role:
                            await modal_interaction.response.send_message("The required role no longer exists.", ephemeral=True)
                            return
                        member = modal_interaction.user
                        if not isinstance(member, discord.Member):
                            try:
                                member = await modal_interaction.guild.fetch_member(modal_interaction.user.id)
                            except Exception:
                                member = None
                        if not member or role not in getattr(member, "roles", []):
                            await modal_interaction.response.send_message(f"Only members with the {role.mention} role can play.", ephemeral=True)
                            return

                    # track unique participants
                    async with DATA_LOCK:
                        game = botref.active_luckynumber.get(sid)
                        if not game:
                            await modal_interaction.response.send_message("This game is no longer active.", ephemeral=True)
                            return
                        entries = game.get('entries', set())
                        if modal_interaction.user.id not in entries:
                            entries.add(modal_interaction.user.id)
                            game['entries'] = entries
                        game['guesses'] = int(game.get('guesses', 0)) + 1
                        botref.active_luckynumber[sid] = game

                    # update entries on the embed
                    try:
                        msg = None
                        if game.get('channel_id'):
                            ch = botref.get_channel(game.get('channel_id'))
                            if ch:
                                msg = await ch.fetch_message(game.get('message_id'))
                        if msg and msg.embeds:
                            old = msg.embeds[0]
                            new = discord.Embed.from_dict(old.to_dict())
                            found_entries = False
                            count = game.get('guesses', 0)
                            for idx, f in enumerate(new.fields):
                                if f.name.lower() == 'guesses':
                                    found_entries = True
                                    new.set_field_at(idx, name=f.name, value=str(count), inline=f.inline)
                            if not found_entries:
                                new.add_field(name="Guesses", value=str(count))
                            await msg.edit(embed=new, view=LuckyNumberView(sid, botref))
                    except Exception:
                        pass

                    # check guess
                    target = game.get('target')
                    if val == target:
                        await modal_interaction.response.send_message("Correct! You got it.", ephemeral=True)
                        await finalize_luckynumber(sid, modal_interaction.user.id, val)
                        return
                    await modal_interaction.response.send_message("Sorry, wrong number. Try again!", ephemeral=True)

            await interaction.response.send_modal(GuessModal())

        guess_btn.callback = guess_cb
        self.add_item(guess_btn)


class MemorySubmitView(discord.ui.View):
    def __init__(self, sid: str, expected_sequence: list, bot):
        super().__init__(timeout=300)
        self.sid = sid
        self.expected_sequence = expected_sequence
        self.bot = bot

    @discord.ui.button(label="Submit Sequence", style=discord.ButtonStyle.primary, custom_id="memory_submit")
    async def submit(self, interaction: discord.Interaction, button: discord.ui.Button):
        # open a modal to capture the sequence text from the winner
        sid = self.sid
        expected = self.expected_sequence
        botref = self.bot

        class MemoryModal(discord.ui.Modal, title="Submit Memory Sequence"):
            seq = discord.ui.TextInput(label="Enter the sequence you saw (paste emojis or text)", style=discord.TextStyle.long, required=True)

            async def on_submit(inner_self, modal_interaction: discord.Interaction):
                answer = inner_self.seq.value.strip()
                # normalize by removing whitespace
                norm_ans = ''.join(answer.split())
                norm_expected = ''.join(expected)
                # determine correctness
                correct = norm_ans == norm_expected
                # announce in original channel
                async with DATA_LOCK:
                    game = botref.active_memory.get(sid)
                    if not game:
                        await modal_interaction.response.send_message("Game no longer active.", ephemeral=True)
                        return
                channel = botref.get_channel(game.get('channel_id'))
                prize = game.get('prize')
                winner_id = game.get('winner')
                if correct:
                    msg = f"<@{winner_id}> submitted the correct sequence and WON {prize}!"
                else:
                    msg = f"<@{winner_id}> submitted: {answer} — incorrect for {prize}."
                if channel:
                    try:
                        await channel.send(msg)
                    except Exception:
                        pass
                if correct:
                    await award_credits_for_prize(botref, [winner_id], prize)
                await modal_interaction.response.send_message("Your answer has been submitted.", ephemeral=True)
                # cleanup
                async with DATA_LOCK:
                    botref.active_memory.pop(sid, None)

        try:
            await interaction.response.send_modal(MemoryModal())
        except Exception as exc:
            logger.exception("Failed to open Memory submission modal: %s", exc)
            # Fallback: ask user to type the sequence directly in this DM
            try:
                await interaction.response.send_message("Could not open modal — please reply here with the sequence you saw within 5 minutes.", ephemeral=True)
            except Exception:
                try:
                    await interaction.followup.send("Please reply here with the sequence you saw within 5 minutes.")
                except Exception:
                    return
            def check(m: discord.Message):
                return m.author.id == interaction.user.id and isinstance(m.channel, discord.DMChannel)

            try:
                msg = await bot.wait_for('message', check=check, timeout=300)
            except asyncio.TimeoutError:
                try:
                    await interaction.followup.send("No submission received in time.")
                except Exception:
                    pass
                return

            answer = msg.content.strip()
            norm_ans = ''.join(answer.split())
            norm_expected = ''.join(expected)
            correct = norm_ans == norm_expected
            async with DATA_LOCK:
                game = botref.active_memory.get(sid)
            if not game:
                try:
                    await msg.channel.send("Game no longer active.")
                except Exception:
                    pass
                return
            channel = botref.get_channel(game.get('channel_id'))
            prize = game.get('prize')
            winner_id = game.get('winner')
            if correct:
                result_msg = f"<@{winner_id}> submitted the correct sequence and WON {prize}!"
            else:
                result_msg = f"<@{winner_id}> submitted: {answer} — incorrect for {prize}."
            if channel:
                try:
                    await channel.send(result_msg)
                except Exception:
                    pass
            try:
                await msg.channel.send("Your answer has been submitted.")
            except Exception:
                pass
            async with DATA_LOCK:
                botref.active_memory.pop(sid, None)


class MemoryConfirmView(discord.ui.View):
    def __init__(self, sid: str, participant_id: int, answer: str):
        super().__init__(timeout=60)
        self.sid = sid
        self.participant_id = participant_id
        self.answer = answer
        self.confirmed = None

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success, custom_id="memory_confirm_yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.participant_id:
            await interaction.response.send_message("This button isn't for you.", ephemeral=True)
            return
        self.confirmed = True
        await interaction.response.send_message("Your answer has been locked in.", ephemeral=True)
        self.stop()

    @discord.ui.button(label="No, redo", style=discord.ButtonStyle.secondary, custom_id="memory_confirm_no")
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.participant_id:
            await interaction.response.send_message("This button isn't for you.", ephemeral=True)
            return
        self.confirmed = False
        # acknowledge the interaction silently; the calling loop will DM the user with redo instructions
        try:
            await interaction.response.defer()
        except Exception:
            try:
                await interaction.response.send_message("Okay — you can reply again with a new sequence.")
            except Exception:
                pass
        self.stop()


class TriviaConfirmView(discord.ui.View):
    def __init__(self, participant_id: int):
        super().__init__(timeout=30)
        self.participant_id = participant_id
        self.confirmed = None

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success, custom_id="trivia_confirm_yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.participant_id:
            await interaction.response.send_message("This button isn't for you.", ephemeral=True)
            return
        self.confirmed = True
        await interaction.response.send_message("Answer locked in.", ephemeral=True)
        self.stop()

    @discord.ui.button(label="No, redo", style=discord.ButtonStyle.secondary, custom_id="trivia_confirm_no")
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.participant_id:
            await interaction.response.send_message("This button isn't for you.", ephemeral=True)
            return
        self.confirmed = False
        try:
            await interaction.response.defer()
        except Exception:
            pass
        self.stop()


class RpsChoiceView(discord.ui.View):
    def __init__(self, sid: str, participant_id: int, bot):
        super().__init__(timeout=300)
        self.sid = sid
        self.participant_id = participant_id
        self.bot = bot

    @discord.ui.button(label="Rock", style=discord.ButtonStyle.primary, custom_id="rps_rock")
    async def rock(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.record_choice(interaction, 'rock')

    @discord.ui.button(label="Paper", style=discord.ButtonStyle.primary, custom_id="rps_paper")
    async def paper(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.record_choice(interaction, 'paper')

    @discord.ui.button(label="Scissors", style=discord.ButtonStyle.primary, custom_id="rps_scissors")
    async def scissors(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.record_choice(interaction, 'scissors')

    async def record_choice(self, interaction: discord.Interaction, choice: str):
        if interaction.user.id != self.participant_id:
            await interaction.response.send_message("This button isn't for you.", ephemeral=True)
            return
        sid = self.sid
        botref = self.bot
        async with DATA_LOCK:
            maze = botref.active_maze.get(sid)
            if not maze:
                await interaction.response.send_message("This maze is no longer active.", ephemeral=True)
                return
            idx = maze.get('index', 0)
            expected = maze.get('sequence', [])
        # compare
        if idx >= len(expected):
            await interaction.response.send_message("Maze already completed.", ephemeral=True)
            return
        expected_choice = expected[idx]
        if choice == expected_choice:
            # correct step
            async with DATA_LOCK:
                maze = botref.active_maze.get(sid)
                if not maze:
                    await interaction.response.send_message("This maze is no longer active.", ephemeral=True)
                    return
                maze['index'] = idx + 1
                botref.active_maze[sid] = maze
            await interaction.response.send_message(f"Correct — step {idx+1}/{len(expected)}. Choose the next step in order.", ephemeral=True)
            # finished?
            if idx + 1 >= len(expected):
                # success
                async with DATA_LOCK:
                    game = botref.active_maze.pop(sid, None)
                if game:
                    channel = botref.get_channel(game.get('channel_id'))
                    prize = game.get('prize')
                    winner = game.get('winner')
                    try:
                        if channel:
                            emoji = random.choice(EMOJI_POOL)
                            await channel.send(f"{emoji} <@{winner}> successfully navigated the maze and WON {prize}! {emoji}")
                    except Exception:
                        pass
                    await award_credits_for_prize(botref, [winner], prize)
            return
        else:
            # wrong — fail
            async with DATA_LOCK:
                game = botref.active_maze.pop(sid, None)
            if game:
                channel = botref.get_channel(game.get('channel_id'))
                prize = game.get('prize')
                winner = game.get('winner')
                try:
                    if channel:
                        await channel.send(f"<@{winner}> chose {choice} (wrong). They failed to navigate the maze for {prize}.")
                except Exception:
                    pass
            await interaction.response.send_message("Wrong choice — you failed the maze. The sequence must be followed in order.", ephemeral=True)
            return
        async with DATA_LOCK:
            rps = self.bot.active_rps.get(self.sid)
            if not rps:
                await interaction.response.send_message("This game is no longer active.", ephemeral=True)
                return
            rps.setdefault('choices', {})[interaction.user.id] = choice
        await interaction.response.send_message(f"You chose {choice}.", ephemeral=True)
        # if both winners have chosen, finalize immediately
        async with DATA_LOCK:
            rps_now = self.bot.active_rps.get(self.sid)
            if rps_now and len(rps_now.get('choices', {})) >= 2:
                try:
                    asyncio.create_task(finalize_rps(self.sid))
                except Exception:
                    pass


class MazeChoiceView(discord.ui.View):
    def __init__(self, sid: str, participant_id: int, expected: list, bot):
        super().__init__(timeout=300)
        self.sid = sid
        self.participant_id = participant_id
        self.expected = expected
        self.bot = bot

        # create per-instance buttons with unique custom_ids to avoid collisions
        left_btn = discord.ui.Button(label="Left", style=discord.ButtonStyle.secondary, custom_id=f"maze_left_{sid}_{participant_id}")
        mid_btn = discord.ui.Button(label="Middle", style=discord.ButtonStyle.secondary, custom_id=f"maze_middle_{sid}_{participant_id}")
        right_btn = discord.ui.Button(label="Right", style=discord.ButtonStyle.secondary, custom_id=f"maze_right_{sid}_{participant_id}")

        async def _left_cb(interaction: discord.Interaction, target=self.participant_id):
            await self.record_choice(interaction, 'left')

        async def _mid_cb(interaction: discord.Interaction, target=self.participant_id):
            await self.record_choice(interaction, 'middle')

        async def _right_cb(interaction: discord.Interaction, target=self.participant_id):
            await self.record_choice(interaction, 'right')

        left_btn.callback = _left_cb
        mid_btn.callback = _mid_cb
        right_btn.callback = _right_cb

        self.add_item(left_btn)
        self.add_item(mid_btn)
        self.add_item(right_btn)

    async def record_choice(self, interaction: discord.Interaction, choice: str):
        try:
            if interaction.user.id != self.participant_id:
                await interaction.response.send_message("This button isn't for you.", ephemeral=True)
                return
            sid = self.sid
            botref = self.bot
            async with DATA_LOCK:
                maze = botref.active_maze.get(sid)
                if not maze:
                    await interaction.response.send_message("This maze is no longer active.", ephemeral=True)
                    return
                idx = maze.get('index', 0)
                expected = maze.get('sequence', [])
            # compare
            if idx >= len(expected):
                await interaction.response.send_message("Maze already completed.", ephemeral=True)
                return
            expected_choice = expected[idx]
            if choice == expected_choice:
                # correct step
                async with DATA_LOCK:
                    maze = botref.active_maze.get(sid)
                    if not maze:
                        await interaction.response.send_message("This maze is no longer active.", ephemeral=True)
                        return
                    maze['index'] = idx + 1
                    botref.active_maze[sid] = maze
                await interaction.response.send_message(f"Correct — step {idx+1}/{len(expected)}. Choose the next step in order.", ephemeral=True)
                # finished?
                if idx + 1 >= len(expected):
                    # success
                    async with DATA_LOCK:
                        game = botref.active_maze.pop(sid, None)
                    if game:
                        channel = botref.get_channel(game.get('channel_id'))
                        prize = game.get('prize')
                        winner = game.get('winner')
                        try:
                            if channel:
                                emoji = random.choice(EMOJI_POOL)
                                await channel.send(f"{emoji} <@{winner}> successfully navigated the maze and WON {prize}! {emoji}")
                        except Exception:
                            pass
                        await award_credits_for_prize(botref, [winner], prize)
                return
            else:
                # wrong — fail
                async with DATA_LOCK:
                    game = botref.active_maze.pop(sid, None)
                if game:
                    channel = botref.get_channel(game.get('channel_id'))
                    prize = game.get('prize')
                    winner = game.get('winner')
                    try:
                        if channel:
                            await channel.send(f"<@{winner}> chose {choice} (wrong). They failed to navigate the maze for {prize}.")
                    except Exception:
                        pass
                await interaction.response.send_message("Wrong choice — you failed the maze. The sequence must be followed in order.", ephemeral=True)
                return
        except Exception:
            logging.exception("Error in MazeChoiceView.record_choice")
            try:
                await interaction.response.send_message("An error occurred handling your choice.", ephemeral=True)
            except Exception:
                try:
                    await interaction.followup.send("An error occurred handling your choice.", ephemeral=True)
                except Exception:
                    pass


class DbdChoiceView(discord.ui.View):
    def __init__(self, sid: str, participant_id: int, bot):
        super().__init__(timeout=300)
        self.sid = sid
        self.participant_id = participant_id
        self.bot = bot

    @discord.ui.button(label="Keep", style=discord.ButtonStyle.secondary, custom_id="dbd_keep")
    async def keep(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.participant_id:
            await interaction.response.send_message("This button isn't for you.", ephemeral=True)
            return
        try:
            await process_dbd_choice(self.bot, self.sid, interaction.user.id, 'keep')
        except Exception:
            pass
        try:
            await interaction.response.send_message("You chose to KEEP.", ephemeral=True)
        except Exception:
            pass
        self.stop()

    @discord.ui.button(label="Double", style=discord.ButtonStyle.primary, custom_id="dbd_double")
    async def double(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.participant_id:
            await interaction.response.send_message("This button isn't for you.", ephemeral=True)
            return
        try:
            await process_dbd_choice(self.bot, self.sid, interaction.user.id, 'double')
        except Exception:
            pass
        try:
            await interaction.response.send_message("You chose to DOUBLE.", ephemeral=True)
        except Exception:
            pass
        self.stop()


class DonChoiceView(discord.ui.View):
    def __init__(self, sid: str, participant_id: int, bot):
        super().__init__(timeout=300)
        self.sid = sid
        self.participant_id = participant_id
        self.bot = bot

    @discord.ui.button(label="Keep", style=discord.ButtonStyle.secondary, custom_id="don_keep")
    async def keep(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.participant_id:
            await interaction.response.send_message("This button isn't for you.", ephemeral=True)
            return
        try:
            asyncio.create_task(process_don_choice(self.bot, self.sid, interaction.user.id, "keep"))
        except Exception:
            pass
        try:
            await interaction.response.send_message("You chose to KEEP.", ephemeral=True)
        except Exception:
            pass
        self.stop()

    @discord.ui.button(label="Double (50/50)", style=discord.ButtonStyle.primary, custom_id="don_double")
    async def double(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.participant_id:
            await interaction.response.send_message("This button isn't for you.", ephemeral=True)
            return
        try:
            asyncio.create_task(process_don_choice(self.bot, self.sid, interaction.user.id, "double"))
        except Exception:
            pass
        try:
            await interaction.response.send_message("You chose to DOUBLE. Good luck!", ephemeral=True)
        except Exception:
            pass
        self.stop()


class ChoiceView(discord.ui.View):
    def __init__(self, sid: str, participant_id: int, bot):
        super().__init__(timeout=300)
        self.sid = sid
        self.participant_id = participant_id
        self.bot = bot

        split_btn = discord.ui.Button(label="Split", style=discord.ButtonStyle.secondary, custom_id=f"sos_split_{sid}_{participant_id}")
        steal_btn = discord.ui.Button(label="Steal", style=discord.ButtonStyle.primary, custom_id=f"sos_steal_{sid}_{participant_id}")

        async def split_cb(interaction: discord.Interaction):
            if interaction.user.id != self.participant_id:
                await interaction.response.send_message("This button isn't for you.", ephemeral=True)
                return
            async with DATA_LOCK:
                sos = self.bot.active_sos.get(self.sid)
                if not sos:
                    await interaction.response.send_message("This game is no longer active.", ephemeral=True)
                    return
                choices = sos.setdefault('choices', {})
                choices[interaction.user.id] = 'split'
                sos['choices'] = choices
                self.bot.active_sos[self.sid] = sos
            try:
                await interaction.response.send_message("You chose SPLIT.", ephemeral=True)
            except Exception:
                pass
            try:
                async with DATA_LOCK:
                    sos_now = self.bot.active_sos.get(self.sid)
                if sos_now and len(sos_now.get('choices', {})) >= 2:
                    asyncio.create_task(finalize_sos(self.sid))
            except Exception:
                pass
            self.stop()

        async def steal_cb(interaction: discord.Interaction):
            if interaction.user.id != self.participant_id:
                await interaction.response.send_message("This button isn't for you.", ephemeral=True)
                return
            async with DATA_LOCK:
                sos = self.bot.active_sos.get(self.sid)
                if not sos:
                    await interaction.response.send_message("This game is no longer active.", ephemeral=True)
                    return
                choices = sos.setdefault('choices', {})
                choices[interaction.user.id] = 'steal'
                sos['choices'] = choices
                self.bot.active_sos[self.sid] = sos
            try:
                await interaction.response.send_message("You chose STEAL.", ephemeral=True)
            except Exception:
                pass
            try:
                async with DATA_LOCK:
                    sos_now = self.bot.active_sos.get(self.sid)
                if sos_now and len(sos_now.get('choices', {})) >= 2:
                    asyncio.create_task(finalize_sos(self.sid))
            except Exception:
                pass
            self.stop()

        split_btn.callback = split_cb
        steal_btn.callback = steal_cb
        self.add_item(split_btn)
        self.add_item(steal_btn)


class ReactAcceptView(discord.ui.View):
    def __init__(self, sid: str, candidate_id: int):
        super().__init__(timeout=60)
        self.sid = sid
        self.candidate_id = candidate_id
        self.result = None

    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success, custom_id="react_accept")
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.candidate_id:
            await interaction.response.send_message("This prompt isn't for you.", ephemeral=True)
            return
        self.result = 'accept'
        await interaction.response.send_message("You accepted — congratulations!", ephemeral=True)
        self.stop()

    @discord.ui.button(label="Pass", style=discord.ButtonStyle.secondary, custom_id="react_pass")
    async def pass_(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.candidate_id:
            await interaction.response.send_message("This prompt isn't for you.", ephemeral=True)
            return
        self.result = 'pass'
        await interaction.response.send_message("You passed the prize.", ephemeral=True)
        self.stop()


class ReactRouletteChoiceView(discord.ui.View):
    def __init__(self, sid: str, winner_id: int, bot, options):
        super().__init__(timeout=300)
        self.sid = sid
        self.winner_id = winner_id
        self.bot = bot
        self.options = options
        self.chosen = None
        self._build_buttons()

    def _build_buttons(self):
        for idx, e in enumerate(self.options):
            btn = discord.ui.Button(label=e, style=discord.ButtonStyle.secondary, custom_id=f"rr_pick_{self.sid}_{idx}")

            async def _cb(interaction: discord.Interaction, emoji=e):
                if interaction.user.id != self.winner_id:
                    await interaction.response.send_message("This button isn't for you.", ephemeral=True)
                    return
                self.chosen = emoji
                try:
                    await interaction.response.send_message(f"You chose {emoji}. Good luck!", ephemeral=True)
                except Exception:
                    pass
                self.stop()

            btn.callback = _cb
            self.add_item(btn)



GUILD_ID = 1463372894984867985
# Allow overriding the guild ID from environment (safer than editing code)
env_gid = os.environ.get('GUILD_ID')
if env_gid:
    try:
        GUILD_ID = int(env_gid)
    except ValueError:
        logging.warning("Invalid GUILD_ID environment variable: %s", env_gid)


class MyBot(commands.Bot):
    def __init__(self, guild_id: Optional[int] = None):
        intents = discord.Intents.default()
        intents.message_content = False
        intents.members = True
        super().__init__(command_prefix='!', intents=intents)
        self.active_giveaways = {}
        self.active_sos = {}
        self.active_memory = {}
        self.active_rps = {}
        self.active_dbd = {}
        self.active_maze = {}
        self.active_reactroulette = {}
        self.active_luckynumber = {}
        self.active_trivia = {}
        self.active_don = {}
        self.active_auctions = {}
        self.credits = load_credits()
        # store recently ended giveaways for rerolls: message_id -> snapshot
        self.recent_giveaways = {}
        self._guild_id = guild_id

    async def setup_hook(self):
        # sync commands to a specific guild for instant availability when provided
        logging.info("setup_hook: syncing commands (guild_id=%s)", self._guild_id)
        try:
            if self._guild_id:
                guild = discord.Object(id=self._guild_id)
                # copy any global commands to the guild and then sync
                try:
                    self.tree.copy_global_to(guild=guild)
                    logging.info("Copied global commands to guild %s", self._guild_id)
                except Exception:
                    logging.exception("Failed to copy global commands to guild %s", self._guild_id)
                synced = await self.tree.sync(guild=guild)
                logging.info("Synced %d commands to guild %s", len(synced), self._guild_id)
                for c in synced:
                    try:
                        logging.info(" - %s (id=%s)", c.name, getattr(c, 'id', None))
                    except Exception:
                        logging.info(" - %s", getattr(c, 'name', str(c)))
            else:
                synced = await self.tree.sync()
                logging.info("Synced %d global commands", len(synced))
                for c in synced:
                    try:
                        logging.info(" - %s (id=%s)", c.name, getattr(c, 'id', None))
                    except Exception:
                        logging.info(" - %s", getattr(c, 'name', str(c)))
        except Exception as e:
            logging.exception("Failed to sync commands: %s", e)


bot = MyBot(guild_id=GUILD_ID)


@bot.event
async def on_ready():
    logging.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)
    # list commands known to the tree at runtime
    try:
        cmds = [c.name for c in bot.tree.walk_commands()]
        logging.info("Commands in tree (%d): %s", len(cmds), ', '.join(cmds))
    except Exception:
        logging.exception("Failed to list commands on ready")


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    # record earliest reactor for active react roulette games
    try:
        async with DATA_LOCK:
            for sid, rr in bot.active_reactroulette.items():
                if rr.get('message_id') == payload.message_id:
                    emoji_str = str(payload.emoji)
                    if emoji_str in rr.get('options', []):
                        # ignore bot reactions
                        if payload.user_id == bot.user.id:
                            return
                        fr = rr.setdefault('first_reactors', {})
                        if fr.get(emoji_str) is None:
                            fr[emoji_str] = payload.user_id
                            bot.active_reactroulette[sid] = rr
                    return
    except Exception:
        logging.exception("on_raw_reaction_add error")


@bot.tree.command(name="gwmake")
async def gwmake(interaction: discord.Interaction):
    """Open a modal to create a giveaway (duration, winners, prize)."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    class GWModal(discord.ui.Modal, title="Create Giveaway"):
        duration = discord.ui.TextInput(label="Duration (e.g. 5d, 2h30m)", style=discord.TextStyle.short, placeholder="5d", required=True)
        winners = discord.ui.TextInput(label="Number of winners", style=discord.TextStyle.short, placeholder="1", required=True, max_length=3)
        prize = discord.ui.TextInput(label="Prize description", style=discord.TextStyle.long, placeholder="Describe the prize", required=True)
        allowed_role = discord.ui.TextInput(
            label="Required role (optional)",
            style=discord.TextStyle.short,
            placeholder="Role mention or ID (leave blank for none)",
            required=False,
            max_length=64,
        )

        async def on_submit(self, modal_interaction: discord.Interaction):
            # parse duration
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            # parse winners
            try:
                w = int(self.winners.value)
                if w < 1:
                    raise ValueError("winners must be >= 1")
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid winners value: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
            role_id = None
            role_input = self.allowed_role.value.strip()
            if role_input:
                role, err = resolve_role_from_input(role_input, modal_interaction.guild)
                if err:
                    await modal_interaction.response.send_message(err, ephemeral=True)
                    return
                role_id = role.id if role else None
            gid = uuid.uuid4().hex[:8]
            end_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)
            gw = {
                'id': gid,
                'creator': modal_interaction.user.id,
                'end_time': end_time.timestamp(),
                'entries': set(),
                'winners': max(1, w),
                'prize': prize_text,
                'mode': 'Giveaway',
                'channel_id': modal_interaction.channel.id if modal_interaction.channel else None,
                'message_id': None,
                'allowed_role_id': role_id,
            }
            view = JoinView(gid, bot)
            embed = discord.Embed(title=prize_text, description="Giveaway", color=discord.Color.blurple())
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            embed.add_field(name="Winners", value=str(gw['winners']))
            embed.add_field(name="Host", value=f"<@{modal_interaction.user.id}>", inline=False)
            embed.add_field(name="Entries", value="0")
            # send in the same channel where the command was invoked
            if gw['channel_id'] is None:
                await modal_interaction.response.send_message("Cannot determine channel to post giveaway.", ephemeral=True)
                return
            channel = bot.get_channel(gw['channel_id'])
            if channel is None:
                await modal_interaction.response.send_message("Channel not found.", ephemeral=True)
                return
            msg = await channel.send(embed=embed, view=view)
            gw['message_id'] = msg.id
            bot.active_giveaways[gid] = gw
            asyncio.create_task(handle_giveaway_end(gid, seconds))
            await modal_interaction.response.send_message(f"Giveaway created (ends <t:{ts}:F> / <t:{ts}:R>).", ephemeral=True)

    await interaction.response.send_modal(GWModal())


async def handle_giveaway_end(gid: str, delay: int):
    await asyncio.sleep(delay)
    async with DATA_LOCK:
        gw = bot.active_giveaways.pop(gid, None)
    if not gw:
        return
    entries = list(gw['entries'])
    channel = bot.get_channel(gw['channel_id'])
    if not channel:
        return
    try:
        message = await channel.fetch_message(gw['message_id'])
    except Exception:
        message = None
    winners = []
    if entries:
        k = min(gw['winners'], len(entries))
        winners = random.sample(entries, k)
    # award credits for winners
    if winners:
        await award_credits_for_prize(bot, winners, gw.get('prize'))
    embed = discord.Embed(title="Giveaway Ended", description=f"Prize: {gw['prize']}")
    embed.add_field(name="Winners", value=', '.join(f"<@{w}>" for w in winners) if winners else "No valid entries")
    embed.add_field(name="Host", value=f"<@{gw.get('creator')}>", inline=False)
    embed.add_field(name="Entries", value=str(len(entries)))
    if message:
        try:
            await message.edit(embed=embed, view=None)
        except Exception:
            await channel.send(embed=embed)
    else:
        await channel.send(embed=embed)
    # save snapshot for possible reroll
    try:
        bot.recent_giveaways[gw['message_id']] = {
            'entries': entries,
            'winners': winners,
            'prize': gw.get('prize'),
            'num_winners': gw.get('winners', 1),
            'creator': gw.get('creator'),
            'channel_id': gw.get('channel_id'),
        }
    except Exception:
        pass
    # Ping winner(s) in channel with a random celebratory emoji
    if winners:
        emoji = random.choice(EMOJI_POOL)
        if len(winners) == 1:
            await channel.send(f"{emoji} Giveaway ended — You've won the giveaway! Congratulations <@{winners[0]}> {emoji}")
        else:
            mentions = ', '.join(f"<@{w}>" for w in winners)
            await channel.send(f"{emoji} Congratulations to the winners: {mentions} — you've won the giveaway! {emoji}")
    # save snapshot for possible reroll
    try:
        bot.recent_giveaways[gw['message_id']] = {
            'entries': entries,
            'winners': winners,
            'prize': gw.get('prize'),
            'num_winners': gw.get('winners', 1),
            'creator': gw.get('creator'),
            'channel_id': gw.get('channel_id'),
        }
    except Exception:
        pass


@bot.tree.command(name="sos")
async def sos(interaction: discord.Interaction):
    """Open a modal to create a Split Or Steal game (duration, prize)."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    class SOSModal(discord.ui.Modal, title="Create Split Or Steal"):
        duration = discord.ui.TextInput(label="Duration (e.g. 5d, 2h30m)", style=discord.TextStyle.short, placeholder="5d", required=True)
        prize = discord.ui.TextInput(label="Prize description", style=discord.TextStyle.long, placeholder="Describe the prize", required=True)
        allowed_role = discord.ui.TextInput(
            label="Required role (optional)",
            style=discord.TextStyle.short,
            placeholder="Role mention or ID (leave blank for none)",
            required=False,
            max_length=64,
        )

        async def on_submit(self, modal_interaction: discord.Interaction):
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a split-or-steal game"
            role_id = None
            role_input = self.allowed_role.value.strip()
            if role_input:
                role, err = resolve_role_from_input(role_input, modal_interaction.guild)
                if err:
                    await modal_interaction.response.send_message(err, ephemeral=True)
                    return
                role_id = role.id if role else None
            sid = uuid.uuid4().hex[:8]
            end_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)
            sos = {
                'id': sid,
                'creator': modal_interaction.user.id,
                'end_time': end_time.timestamp(),
                'entries': set(),
                'prize': prize_text,
                'mode': 'Split or Steal',
                'channel_id': modal_interaction.channel.id if modal_interaction.channel else None,
                'message_id': None,
                'choices': {},
                'winners': [],
                'num_winners': 2,
                'allowed_role_id': role_id,
            }
            view = JoinView(sid, bot)
            embed = discord.Embed(title=prize_text, description="Split Or Steal: Join")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            embed.add_field(name="Winners", value="2")
            embed.add_field(name="Host", value=f"<@{modal_interaction.user.id}>", inline=False)
            embed.add_field(name="Entries", value="0")
            if sos['channel_id'] is None:
                await modal_interaction.response.send_message("Cannot determine channel to post SOS.", ephemeral=True)
                return
            channel = bot.get_channel(sos['channel_id'])
            if channel is None:
                await modal_interaction.response.send_message("Channel not found.", ephemeral=True)
                return
            msg = await channel.send(embed=embed, view=view)
            sos['message_id'] = msg.id
            bot.active_giveaways[sid] = sos
            bot.active_sos[sid] = sos
            asyncio.create_task(handle_sos_end(sid, seconds))

    await interaction.response.send_modal(SOSModal())


@bot.tree.command(name="rps")
async def rps(interaction: discord.Interaction):
    """Open a modal to create a Rock Paper Scissors game (duration, prize)."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    class RpsModal(discord.ui.Modal, title="Create Rock Paper Scissors"):
        duration = discord.ui.TextInput(label="Duration (e.g. 5d, 2h30m)", style=discord.TextStyle.short, placeholder="5d", required=True)
        prize = discord.ui.TextInput(label="Prize description", style=discord.TextStyle.long, placeholder="Describe the prize", required=True)
        allowed_role = discord.ui.TextInput(
            label="Required role (optional)",
            style=discord.TextStyle.short,
            placeholder="Role mention or ID (leave blank for none)",
            required=False,
            max_length=64,
        )

        async def on_submit(self, modal_interaction: discord.Interaction):
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
            role_id = None
            role_input = self.allowed_role.value.strip()
            if role_input:
                role, err = resolve_role_from_input(role_input, modal_interaction.guild)
                if err:
                    await modal_interaction.response.send_message(err, ephemeral=True)
                    return
                role_id = role.id if role else None
            sid = uuid.uuid4().hex[:8]
            end_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)
            rps_game = {
                'id': sid,
                'creator': modal_interaction.user.id,
                'end_time': end_time.timestamp(),
                'entries': set(),
                'prize': prize_text,
                'mode': 'Rock Paper Scissors',
                'channel_id': modal_interaction.channel.id if modal_interaction.channel else None,
                'message_id': None,
                'choices': {},
                'winners': [],
                'num_winners': 2,
                'allowed_role_id': role_id,
            }
            view = JoinView(sid, bot)
            embed = discord.Embed(title=prize_text, description="Rock Paper Scissors: Join")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            embed.add_field(name="Winners", value="2")
            embed.add_field(name="Host", value=f"<@{modal_interaction.user.id}>", inline=False)
            embed.add_field(name="Entries", value="0")
            if rps_game['channel_id'] is None:
                await modal_interaction.response.send_message("Cannot determine channel to post RPS.", ephemeral=True)
                return
            channel = bot.get_channel(rps_game['channel_id'])
            if channel is None:
                await modal_interaction.response.send_message("Channel not found.", ephemeral=True)
                return
            msg = await channel.send(embed=embed, view=view)
            rps_game['message_id'] = msg.id
            bot.active_giveaways[sid] = rps_game
            bot.active_rps[sid] = rps_game
            asyncio.create_task(handle_rps_end(sid, seconds))
            await modal_interaction.response.send_message(f"RPS created (ends <t:{ts}:F> / <t:{ts}:R>).", ephemeral=True)

    await interaction.response.send_modal(RpsModal())


@bot.tree.command(name="dbd")
async def dbd(interaction: discord.Interaction):
    """Open a modal to create a Double Down game (duration, prize)."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    class DbdModal(discord.ui.Modal, title="Create Double Down (DBD)"):
        duration = discord.ui.TextInput(label="Duration (e.g. 5d, 2h30m)", style=discord.TextStyle.short, placeholder="5d", required=True)
        prize = discord.ui.TextInput(label="Prize description", style=discord.TextStyle.long, placeholder="Describe the prize", required=True)
        allowed_role = discord.ui.TextInput(
            label="Required role (optional)",
            style=discord.TextStyle.short,
            placeholder="Role mention or ID (leave blank for none)",
            required=False,
            max_length=64,
        )

        async def on_submit(self, modal_interaction: discord.Interaction):
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
            role_id = None
            role_input = self.allowed_role.value.strip()
            if role_input:
                role, err = resolve_role_from_input(role_input, modal_interaction.guild)
                if err:
                    await modal_interaction.response.send_message(err, ephemeral=True)
                    return
                role_id = role.id if role else None
            sid = uuid.uuid4().hex[:8]
            end_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)
            dbd_game = {
                'id': sid,
                'creator': modal_interaction.user.id,
                'end_time': end_time.timestamp(),
                'entries': set(),
                'prize': prize_text,
                'base_prize': prize_text,
                'multiplier': 1,
                'mode': 'Double Down',
                'channel_id': modal_interaction.channel.id if modal_interaction.channel else None,
                'message_id': None,
                'attempted': [],
                'choices': {},
                'allowed_role_id': role_id,
            }
            view = JoinView(sid, bot)
            embed = discord.Embed(title=prize_text, description="Double Down: Join")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            embed.add_field(name="Winners", value="1")
            embed.add_field(name="Host", value=f"<@{modal_interaction.user.id}>", inline=False)
            embed.add_field(name="Entries", value="0")
            if dbd_game['channel_id'] is None:
                await modal_interaction.response.send_message("Cannot determine channel to post DBD.", ephemeral=True)
                return
            channel = bot.get_channel(dbd_game['channel_id'])
            if channel is None:
                await modal_interaction.response.send_message("Channel not found.", ephemeral=True)
                return
            msg = await channel.send(embed=embed, view=view)
            dbd_game['message_id'] = msg.id
            bot.active_giveaways[sid] = dbd_game
            bot.active_dbd[sid] = dbd_game
            asyncio.create_task(handle_dbd_end(sid, seconds))
            await modal_interaction.response.send_message(f"Double Down created (ends <t:{ts}:F> / <t:{ts}:R>).", ephemeral=True)

    await interaction.response.send_modal(DbdModal())


@bot.tree.command(name="memory")
async def memory(interaction: discord.Interaction):
    """Create a Memory Sequence minigame (1 winner)."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    class MemoryModalCreate(discord.ui.Modal, title="Create Memory Game"):
        duration = discord.ui.TextInput(label="Duration (e.g. 30s, 2m)", style=discord.TextStyle.short, placeholder="30s", required=True)
        prize = discord.ui.TextInput(label="Prize description", style=discord.TextStyle.long, placeholder="Describe the prize", required=True)
        allowed_role = discord.ui.TextInput(
            label="Required role (optional)",
            style=discord.TextStyle.short,
            placeholder="Role mention or ID (leave blank for none)",
            required=False,
            max_length=64,
        )

        async def on_submit(self, modal_interaction: discord.Interaction):
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
            role_id = None
            role_input = self.allowed_role.value.strip()
            if role_input:
                role, err = resolve_role_from_input(role_input, modal_interaction.guild)
                if err:
                    await modal_interaction.response.send_message(err, ephemeral=True)
                    return
                role_id = role.id if role else None
            sid = uuid.uuid4().hex[:8]
            end_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)
            # generate sequence of colored circles (memorize by color)
            seq = random.choices(MEMORY_COLORS, k=5)
            memory_game = {
                'id': sid,
                'creator': modal_interaction.user.id,
                'end_time': end_time.timestamp(),
                'entries': set(),
                'prize': prize_text,
                'mode': 'Memory',
                'channel_id': modal_interaction.channel.id if modal_interaction.channel else None,
                'message_id': None,
                'sequence': seq,
                'winner': None,
                'allowed_role_id': role_id,
            }
            view = JoinView(sid, bot)
            embed = discord.Embed(title=prize_text, description="Memory: Join")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            embed.add_field(name="Winners", value="1")
            embed.add_field(name="Host", value=f"<@{modal_interaction.user.id}>", inline=False)
            embed.add_field(name="Entries", value="0")
            if memory_game['channel_id'] is None:
                await modal_interaction.response.send_message("Cannot determine channel to post Memory game.", ephemeral=True)
                return
            channel = bot.get_channel(memory_game['channel_id'])
            if channel is None:
                await modal_interaction.response.send_message("Channel not found.", ephemeral=True)
                return
            msg = await channel.send(embed=embed, view=view)
            memory_game['message_id'] = msg.id
            bot.active_giveaways[sid] = memory_game
            bot.active_memory[sid] = memory_game
            asyncio.create_task(handle_memory_end(sid, seconds))
            await modal_interaction.response.send_message(f"Memory game created (ends <t:{ts}:F> / <t:{ts}:R>).", ephemeral=True)

    await interaction.response.send_modal(MemoryModalCreate())


@bot.tree.command(name="maze")
async def maze(interaction: discord.Interaction):
    """Create a Maze Runner minigame (1 winner who must navigate choices)."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    class MazeModal(discord.ui.Modal, title="Create Maze Runner"):
        duration = discord.ui.TextInput(label="Duration (e.g. 30s, 2m)", style=discord.TextStyle.short, placeholder="30s", required=True)
        prize = discord.ui.TextInput(label="Prize description", style=discord.TextStyle.long, placeholder="Describe the prize", required=True)
        length = discord.ui.TextInput(label="Maze length (steps)", style=discord.TextStyle.short, placeholder="5", required=True, max_length=3)
        allowed_role = discord.ui.TextInput(
            label="Required role (optional)",
            style=discord.TextStyle.short,
            placeholder="Role mention or ID (leave blank for none)",
            required=False,
            max_length=64,
        )

        async def on_submit(self, modal_interaction: discord.Interaction):
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            try:
                length = int(self.length.value)
                if length < 1 or length > 12:
                    raise ValueError("length must be between 1 and 12")
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid length: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
            role_id = None
            role_input = self.allowed_role.value.strip()
            if role_input:
                role, err = resolve_role_from_input(role_input, modal_interaction.guild)
                if err:
                    await modal_interaction.response.send_message(err, ephemeral=True)
                    return
                role_id = role.id if role else None
            sid = uuid.uuid4().hex[:8]
            end_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)
            seq = [random.choice(['left','middle','right']) for _ in range(length)]
            maze_game = {
                'id': sid,
                'creator': modal_interaction.user.id,
                'end_time': end_time.timestamp(),
                'entries': set(),
                'prize': prize_text,
                'mode': 'Maze Runner',
                'channel_id': modal_interaction.channel.id if modal_interaction.channel else None,
                'message_id': None,
                'sequence': seq,
                'winner': None,
                'index': 0,
                'allowed_role_id': role_id,
            }
            view = JoinView(sid, bot)
            embed = discord.Embed(title=prize_text, description="Maze Runner: Join")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            embed.add_field(name="Winners", value="1")
            embed.add_field(name="Host", value=f"<@{modal_interaction.user.id}>", inline=False)
            embed.add_field(name="Entries", value="0")
            if maze_game['channel_id'] is None:
                await modal_interaction.response.send_message("Cannot determine channel to post Maze game.", ephemeral=True)
                return
            channel = bot.get_channel(maze_game['channel_id'])
            if channel is None:
                await modal_interaction.response.send_message("Channel not found.", ephemeral=True)
                return
            msg = await channel.send(embed=embed, view=view)
            maze_game['message_id'] = msg.id
            bot.active_giveaways[sid] = maze_game
            bot.active_maze[sid] = maze_game
            asyncio.create_task(handle_maze_end(sid, seconds))
            await modal_interaction.response.send_message(f"Maze created (ends <t:{ts}:F> / <t:{ts}:R>).", ephemeral=True)

    await interaction.response.send_modal(MazeModal())


@bot.tree.command(name="luckynumber")
async def luckynumber(interaction: discord.Interaction):
    """Create a Lucky Number game (guess 1-100)."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return

    class LuckyNumberModal(discord.ui.Modal, title="Create Lucky Number"):
        duration = discord.ui.TextInput(label="Duration (e.g. 30s, 2m)", style=discord.TextStyle.short, placeholder="2m", required=True)
        prize = discord.ui.TextInput(label="Prize description", style=discord.TextStyle.long, placeholder="Describe the prize", required=True)
        target_number = discord.ui.TextInput(
            label="Correct number (1-100, optional)",
            style=discord.TextStyle.short,
            placeholder="Leave blank for random",
            required=False,
            max_length=3,
        )
        allowed_role = discord.ui.TextInput(
            label="Required role (optional)",
            style=discord.TextStyle.short,
            placeholder="Role mention or ID (leave blank for none)",
            required=False,
            max_length=64,
        )

        async def on_submit(self, modal_interaction: discord.Interaction):
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
            role_id = None
            role_input = self.allowed_role.value.strip()
            if role_input:
                role, err = resolve_role_from_input(role_input, modal_interaction.guild)
                if err:
                    await modal_interaction.response.send_message(err, ephemeral=True)
                    return
                role_id = role.id if role else None

            sid = uuid.uuid4().hex[:8]
            end_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)
            target_input = self.target_number.value.strip()
            if target_input:
                try:
                    target = int(target_input)
                    if target < 1 or target > 100:
                        raise ValueError("Out of range")
                except Exception:
                    await modal_interaction.response.send_message("Correct number must be a whole number from 1 to 100.", ephemeral=True)
                    return
            else:
                target = random.randint(1, 100)
            ln_game = {
                'id': sid,
                'creator': modal_interaction.user.id,
                'end_time': end_time.timestamp(),
                'entries': set(),
                'guesses': 0,
                'prize': prize_text,
                'mode': 'Lucky Number',
                'channel_id': modal_interaction.channel.id if modal_interaction.channel else None,
                'message_id': None,
                'target': target,
                'allowed_role_id': role_id,
            }

            view = LuckyNumberView(sid, bot)
            embed = discord.Embed(title=prize_text, description="Lucky Number: Join")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)", inline=True)
            embed.add_field(name="Range", value="1-100", inline=True)
            embed.add_field(name="Winners", value="1", inline=True)
            embed.add_field(name="Host", value=f"<@{modal_interaction.user.id}>", inline=False)
            embed.add_field(name="Guesses", value="0", inline=False)

            if ln_game['channel_id'] is None:
                await modal_interaction.response.send_message("Cannot determine channel to post Lucky Number.", ephemeral=True)
                return
            channel = bot.get_channel(ln_game['channel_id'])
            if channel is None:
                await modal_interaction.response.send_message("Channel not found.", ephemeral=True)
                return
            msg = await channel.send(embed=embed, view=view)
            ln_game['message_id'] = msg.id
            bot.active_luckynumber[sid] = ln_game
            asyncio.create_task(handle_luckynumber_end(sid, seconds))
            await modal_interaction.response.send_message(f"Lucky Number created (ends <t:{ts}:F> / <t:{ts}:R>).", ephemeral=True)

    await interaction.response.send_modal(LuckyNumberModal())


@bot.tree.command(name="trivia")
async def trivia(interaction: discord.Interaction):
    """Create a Trivia game (random winner answers a question)."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return

    class TriviaModal(discord.ui.Modal, title="Create Trivia"):
        duration = discord.ui.TextInput(label="Duration (e.g. 30s, 2m)", style=discord.TextStyle.short, placeholder="2m", required=True)
        prize = discord.ui.TextInput(label="Prize description", style=discord.TextStyle.long, placeholder="Describe the prize", required=True)
        difficulty = discord.ui.TextInput(
            label="Difficulty (easy/medium/hard/nightmare)",
            style=discord.TextStyle.short,
            placeholder="easy",
            required=True,
            max_length=10,
        )
        allowed_role = discord.ui.TextInput(
            label="Required role (optional)",
            style=discord.TextStyle.short,
            placeholder="Role mention or ID (leave blank for none)",
            required=False,
            max_length=64,
        )

        async def on_submit(self, modal_interaction: discord.Interaction):
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
            diff = self.difficulty.value.strip().lower()
            if diff not in TRIVIA_BANK:
                await modal_interaction.response.send_message("Difficulty must be easy, medium, hard, or nightmare.", ephemeral=True)
                return
            role_id = None
            role_input = self.allowed_role.value.strip()
            if role_input:
                role, err = resolve_role_from_input(role_input, modal_interaction.guild)
                if err:
                    await modal_interaction.response.send_message(err, ephemeral=True)
                    return
                role_id = role.id if role else None

            question = random.choice(TRIVIA_BANK[diff])
            sid = uuid.uuid4().hex[:8]
            end_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)
            game = {
                'id': sid,
                'creator': modal_interaction.user.id,
                'end_time': end_time.timestamp(),
                'entries': set(),
                'prize': prize_text,
                'mode': 'Trivia',
                'channel_id': modal_interaction.channel.id if modal_interaction.channel else None,
                'message_id': None,
                'difficulty': diff,
                'question': question.get('q'),
                'answer': question.get('a'),
                'allowed_role_id': role_id,
            }

            view = JoinView(sid, bot)
            embed = discord.Embed(title=prize_text, description="Trivia: Join")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)", inline=True)
            embed.add_field(name="Difficulty", value=diff.title(), inline=True)
            embed.add_field(name="Host", value=f"<@{modal_interaction.user.id}>", inline=False)
            embed.add_field(name="Entries", value="0", inline=False)

            if game['channel_id'] is None:
                await modal_interaction.response.send_message("Cannot determine channel to post Trivia.", ephemeral=True)
                return
            channel = bot.get_channel(game['channel_id'])
            if channel is None:
                await modal_interaction.response.send_message("Channel not found.", ephemeral=True)
                return
            msg = await channel.send(embed=embed, view=view)
            game['message_id'] = msg.id
            bot.active_trivia[sid] = game
            asyncio.create_task(handle_trivia_end(sid, seconds))
            await modal_interaction.response.send_message(f"Trivia created (ends <t:{ts}:F> / <t:{ts}:R>).", ephemeral=True)

    await interaction.response.send_modal(TriviaModal())


@bot.tree.command(name="don")
async def don(interaction: discord.Interaction):
    """Create a Double or Nothing game (winner chooses keep or 50/50 double)."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return

    class DonModal(discord.ui.Modal, title="Create Double or Nothing"):
        duration = discord.ui.TextInput(label="Duration (e.g. 30s, 2m)", style=discord.TextStyle.short, placeholder="2m", required=True)
        prize = discord.ui.TextInput(label="Prize description", style=discord.TextStyle.long, placeholder="Describe the prize", required=True)
        risk_mode = discord.ui.TextInput(
            label="Risk mode (coin / roulette / wheel)",
            style=discord.TextStyle.short,
            placeholder="coin",
            required=True,
            max_length=12,
        )
        allowed_role = discord.ui.TextInput(
            label="Required role (optional)",
            style=discord.TextStyle.short,
            placeholder="Role mention or ID (leave blank for none)",
            required=False,
            max_length=64,
        )

        async def on_submit(self, modal_interaction: discord.Interaction):
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
            mode_input = self.risk_mode.value.strip().lower()
            if mode_input in ("coin", "flip", "coinflip"):
                risk_mode = "coin"
            elif mode_input in ("roulette", "rr", "russian"):
                risk_mode = "roulette"
            elif mode_input in ("wheel", "wheel of fate", "fate"):
                risk_mode = "wheel"
            else:
                await modal_interaction.response.send_message("Risk mode must be coin, roulette, or wheel.", ephemeral=True)
                return
            role_id = None
            role_input = self.allowed_role.value.strip()
            if role_input:
                role, err = resolve_role_from_input(role_input, modal_interaction.guild)
                if err:
                    await modal_interaction.response.send_message(err, ephemeral=True)
                    return
                role_id = role.id if role else None

            sid = uuid.uuid4().hex[:8]
            end_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)
            game = {
                'id': sid,
                'creator': modal_interaction.user.id,
                'end_time': end_time.timestamp(),
                'entries': set(),
                'prize': prize_text,
                'base_prize': prize_text,
                'multiplier': 1,
                'mode': 'Double or Nothing',
                'risk_mode': risk_mode,
                'channel_id': modal_interaction.channel.id if modal_interaction.channel else None,
                'message_id': None,
                'allowed_role_id': role_id,
                'winner': None,
                'awaiting_choice': False,
                'finalized': False,
            }

            view = JoinView(sid, bot)
            embed = discord.Embed(title=prize_text, description="Double or Nothing: Join")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)", inline=True)
            embed.add_field(name="Winners", value="1", inline=True)
            if risk_mode == "coin":
                risk_label = "Coin Flip (2x, 50%)"
            elif risk_mode == "roulette":
                risk_label = "Roulette (5x, 20%)"
            else:
                risk_label = "Wheel of Fate (???)"
            embed.add_field(name="Risk", value=risk_label, inline=True)
            embed.add_field(name="Host", value=f"<@{modal_interaction.user.id}>", inline=False)
            embed.add_field(name="Entries", value="0", inline=False)

            if game['channel_id'] is None:
                await modal_interaction.response.send_message("Cannot determine channel to post Double or Nothing.", ephemeral=True)
                return
            channel = bot.get_channel(game['channel_id'])
            if channel is None:
                await modal_interaction.response.send_message("Channel not found.", ephemeral=True)
                return
            msg = await channel.send(embed=embed, view=view)
            game['message_id'] = msg.id
            bot.active_don[sid] = game
            asyncio.create_task(handle_don_end(sid, seconds))
            await modal_interaction.response.send_message(f"Double or Nothing created (ends <t:{ts}:F> / <t:{ts}:R>).", ephemeral=True)

    await interaction.response.send_modal(DonModal())


@bot.tree.command(name="sync")
@app_commands.describe(global_sync="If true, sync commands globally instead of to the guild")
async def sync(interaction: discord.Interaction, global_sync: bool = False):
    # admin-only
    if interaction.guild and not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("You must be a server administrator to run this.", ephemeral=True)
        return
    await interaction.response.send_message("Syncing commands...", ephemeral=True)
    try:
        if global_sync:
            synced = await bot.tree.sync()
            logging.info("Manually synced %d global commands", len(synced))
            names = ', '.join(c.name for c in synced)
            await interaction.edit_original_response(content=f"Synced {len(synced)} global commands: {names}")
        else:
            gid = bot._guild_id or (interaction.guild.id if interaction.guild else None)
            if not gid:
                await interaction.edit_original_response(content="No guild id available to sync to.")
                return
            guild_obj = discord.Object(id=gid)
            synced = await bot.tree.sync(guild=guild_obj)
            logging.info("Manually synced %d commands to guild %s", len(synced), gid)
            names = ', '.join(c.name for c in synced)
            await interaction.edit_original_response(content=f"Synced {len(synced)} commands to guild {gid}: {names}")
    except Exception as e:
        logging.exception("Manual sync failed: %s", e)
        await interaction.edit_original_response(content=f"Sync failed: {e}")


@bot.tree.command(name="addcredit")
@app_commands.describe(member="Member to add credits to", amount="Credits to add")
async def addcredit(interaction: discord.Interaction, member: discord.Member, amount: int):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    if amount <= 0:
        await interaction.response.send_message("Amount must be positive.", ephemeral=True)
        return
    async with DATA_LOCK:
        key = str(member.id)
        bot.credits[key] = int(bot.credits.get(key, 0)) + int(amount)
        save_credits(bot.credits)
        new_total = bot.credits[key]
    await interaction.response.send_message(f"Added {amount} credits to {member.mention}. New total: {new_total}.", ephemeral=True)


@bot.tree.command(name="removecredit")
@app_commands.describe(member="Member to remove credits from", amount="Credits to remove")
async def removecredit(interaction: discord.Interaction, member: discord.Member, amount: int):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    if amount <= 0:
        await interaction.response.send_message("Amount must be positive.", ephemeral=True)
        return
    async with DATA_LOCK:
        key = str(member.id)
        bot.credits[key] = max(0, int(bot.credits.get(key, 0)) - int(amount))
        save_credits(bot.credits)
        new_total = bot.credits[key]
    await interaction.response.send_message(f"Removed {amount} credits from {member.mention}. New total: {new_total}.", ephemeral=True)


@bot.tree.command(name="creditcheck")
@app_commands.describe(member="Member to check (optional)")
async def creditcheck(interaction: discord.Interaction, member: Optional[discord.Member] = None):
    target = member or interaction.user
    async with DATA_LOCK:
        total = int(bot.credits.get(str(target.id), 0))
    await interaction.response.send_message(f"{target.mention} has {total} credits.", ephemeral=True)


@bot.tree.command(name="gwend")
@app_commands.describe(message_id="The message ID of the giveaway embed to end early")
async def gwend(interaction: discord.Interaction, message_id: str):
    """End a giveaway early by providing its message ID. Admins or the giveaway creator only."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    await interaction.response.send_message("Ending giveaway...", ephemeral=True)
    try:
        mid = int(message_id)
    except Exception:
        await interaction.edit_original_response(content="Invalid message id. Provide the numeric message id.")
        return

    async with DATA_LOCK:
        # find giveaway by message id
        found_gid = None
        for gid, gw in list(bot.active_giveaways.items()):
            if gw.get('message_id') == mid:
                found_gid = gid
                found_gw = gw
                break
        if not found_gid:
            await interaction.edit_original_response(content="No active giveaway found with that message id.")
            return
        # permission check: admins only
        if not (interaction.guild and interaction.user.guild_permissions.administrator):
            await interaction.edit_original_response(content="You must be a server administrator to end giveaways.")
            return
        # remove giveaway to prevent scheduled handler from running
        gw = bot.active_giveaways.pop(found_gid, None)

    if not gw:
        await interaction.edit_original_response(content="Giveaway already ended or not found.")
        return

    entries = list(gw.get('entries', []))
    channel = bot.get_channel(gw.get('channel_id'))
    if not channel:
        await interaction.edit_original_response(content="Channel for this giveaway could not be found.")
        return

    try:
        message = await channel.fetch_message(gw.get('message_id'))
    except Exception:
        message = None

    winners = []
    if entries:
        k = min(gw.get('winners', 1), len(entries))
        winners = random.sample(entries, k)
    if winners:
        await award_credits_for_prize(bot, winners, gw.get('prize'))

    embed = discord.Embed(title="Giveaway Ended Early", description=f"Prize: {gw.get('prize')}")
    embed.add_field(name="Winners", value=', '.join(f"<@{w}>" for w in winners) if winners else "No valid entries")
    embed.add_field(name="Host", value=f"<@{gw.get('creator')}>", inline=False)
    embed.add_field(name="Entries", value=str(len(entries)))

    if message:
        try:
            await message.edit(embed=embed, view=None)
        except Exception:
            await channel.send(embed=embed)
    else:
        await channel.send(embed=embed)

    # Announce and ping winners with emoji
    emoji = random.choice(EMOJI_POOL)
    if winners:
        if len(winners) == 1:
            await channel.send(f"{emoji} Giveaway ended early — You've won the giveaway! Congratulations <@{winners[0]}> {emoji}")
        else:
            mentions = ', '.join(f"<@{w}>" for w in winners)
            await channel.send(f"{emoji} Giveaway ended early — Congratulations to the winners: {mentions} — you've won the giveaway! {emoji}")
    else:
        await channel.send(f"{emoji} Giveaway ended early — No valid entries, no winners.")

    # save snapshot for possible reroll
    try:
        bot.recent_giveaways[gw['message_id']] = {
            'entries': entries,
            'winners': winners,
            'prize': gw.get('prize'),
            'num_winners': gw.get('winners', 1),
            'creator': gw.get('creator'),
            'channel_id': gw.get('channel_id'),
        }
    except Exception:
        pass

    await interaction.edit_original_response(content="Giveaway ended early.")


@bot.tree.command(name="reroll")
@app_commands.describe(message_id="The message ID of the ended giveaway to reroll")
async def reroll(interaction: discord.Interaction, message_id: str):
    """Reroll a finished giveaway by message ID. Admins or the giveaway creator only."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    await interaction.response.send_message("Rerolling giveaway...", ephemeral=True)
    try:
        mid = int(message_id)
    except Exception:
        await interaction.edit_original_response(content="Invalid message id.")
        return

    snapshot = bot.recent_giveaways.get(mid)
    if not snapshot:
        await interaction.edit_original_response(content="No recent giveaway found with that message id.")
        return

    # permission check: admins only
    if not (interaction.guild and interaction.user.guild_permissions.administrator):
        await interaction.edit_original_response(content="You must be a server administrator to reroll giveaways.")
        return

    entries = list(snapshot.get('entries', []))
    prev_winners = list(snapshot.get('winners', []))
    k = int(snapshot.get('num_winners', 1))
    if not entries:
        await interaction.edit_original_response(content="No entries to choose from.")
        return

    # prefer to exclude previous winners if possible
    pool = [e for e in entries if e not in prev_winners]
    if len(pool) < k:
        pool = entries

    new_winners = random.sample(pool, min(k, len(pool)))
    if new_winners:
        await award_credits_for_prize(bot, new_winners, snapshot.get('prize'))

    # update snapshot winners
    snapshot['winners'] = new_winners

    # announce in channel
    channel = bot.get_channel(snapshot.get('channel_id'))
    emoji = random.choice(EMOJI_POOL)
    if not channel:
        await interaction.edit_original_response(content="Could not find the channel for this giveaway.")
        return

    if len(new_winners) == 1:
        await channel.send(f"{interaction.user.mention} rerolled — the new winner is <@{new_winners[0]}> {emoji}")
    else:
        mentions = ', '.join(f"<@{w}>" for w in new_winners)
        await channel.send(f"{interaction.user.mention} rerolled — the new winners are: {mentions} {emoji}")

    # try editing the original message embed to show new winners
    try:
        msg = await channel.fetch_message(mid)
        embed = discord.Embed(title="Giveaway Rerolled", description=f"Prize: {snapshot.get('prize')}")
        embed.add_field(name="Winners", value=', '.join(f"<@{w}>" for w in new_winners))
        embed.add_field(name="Host", value=f"<@{snapshot.get('creator')}>", inline=False)
        embed.add_field(name="Entries", value=str(len(entries)))
        try:
            await msg.edit(embed=embed, view=None)
        except Exception:
            pass
    except Exception:
        pass

    await interaction.edit_original_response(content="Reroll complete.")



async def handle_sos_end(sid: str, delay: int):
    await asyncio.sleep(delay)
    async with DATA_LOCK:
        sos = bot.active_sos.get(sid)
        if not sos:
            return
        # remove from giveaway tracker but keep sos active for choices
        bot.active_giveaways.pop(sid, None)
    if not sos:
        return
    entries = list(sos['entries'])
    channel = bot.get_channel(sos['channel_id'])
    if not channel:
        return
    try:
        message = await channel.fetch_message(sos['message_id'])
    except Exception:
        message = None
    winners = []
    if len(entries) >= 2:
        winners = random.sample(entries, 2)
    elif len(entries) == 1:
        winners = entries
    embed = discord.Embed(title="Split Or Steal Ended", description=f"Prize: {sos['prize']}")
    embed.add_field(name="Entries", value=str(len(entries)))
    if not winners or len(winners) < 2:
        embed.add_field(name="Result", value="Not enough participants — no game")
        if message:
            await message.edit(embed=embed, view=None)
        else:
            await channel.send(embed=embed)
        return
    sos['winners'] = winners
    # announce who is competing in-channel
    try:
        emoji = random.choice(EMOJI_POOL)
        prize_text = sos.get('prize')
        if len(winners) == 2:
            await channel.send(
                f"{emoji} <@{winners[0]}> and <@{winners[1]}> are competing for {prize_text}! Check your DMs to respond."
            )
        else:
            await channel.send(
                f"{emoji} <@{winners[0]}> is the sole contender for {prize_text} — check your DMs."
            )
    except Exception:
        pass

    # DM both winners with choice buttons
    for uid in winners:
        user = bot.get_user(uid)
        if not user:
            try:
                user = await bot.fetch_user(uid)
            except Exception:
                user = None
        if not user:
            continue
        other_mentions = ' and '.join([f'<@{w}>' for w in winners if w != uid])
        # build a fresh view inline to avoid accidental mixing with other Views
        local_view = discord.ui.View(timeout=300)
        split_btn = discord.ui.Button(label="Split", style=discord.ButtonStyle.secondary, custom_id=f"sos_split_{sid}_{uid}")
        steal_btn = discord.ui.Button(label="Steal", style=discord.ButtonStyle.primary, custom_id=f"sos_steal_{sid}_{uid}")

        async def _split_cb(interaction: discord.Interaction, target_uid=uid):
            if interaction.user.id != target_uid:
                await interaction.response.send_message("This button isn't for you.", ephemeral=True)
                return
            async with DATA_LOCK:
                sos_now = bot.active_sos.get(sid)
                if not sos_now:
                    await interaction.response.send_message("This game is no longer active.", ephemeral=True)
                    return
                choices = sos_now.setdefault('choices', {})
                choices[interaction.user.id] = 'split'
                sos_now['choices'] = choices
                bot.active_sos[sid] = sos_now
            try:
                await interaction.response.send_message("You chose SPLIT.", ephemeral=True)
            except Exception:
                pass
            try:
                async with DATA_LOCK:
                    sos_now2 = bot.active_sos.get(sid)
                if sos_now2 and len(sos_now2.get('choices', {})) >= 2:
                    asyncio.create_task(finalize_sos(sid))
            except Exception:
                pass

        async def _steal_cb(interaction: discord.Interaction, target_uid=uid):
            if interaction.user.id != target_uid:
                await interaction.response.send_message("This button isn't for you.", ephemeral=True)
                return
            async with DATA_LOCK:
                sos_now = bot.active_sos.get(sid)
                if not sos_now:
                    await interaction.response.send_message("This game is no longer active.", ephemeral=True)
                    return
                choices = sos_now.setdefault('choices', {})
                choices[interaction.user.id] = 'steal'
                sos_now['choices'] = choices
                bot.active_sos[sid] = sos_now
            try:
                await interaction.response.send_message("You chose STEAL.", ephemeral=True)
            except Exception:
                pass
            try:
                async with DATA_LOCK:
                    sos_now2 = bot.active_sos.get(sid)
                if sos_now2 and len(sos_now2.get('choices', {})) >= 2:
                    asyncio.create_task(finalize_sos(sid))
            except Exception:
                pass

        split_btn.callback = _split_cb
        steal_btn.callback = _steal_cb
        local_view.add_item(split_btn)
        local_view.add_item(steal_btn)
        try:
            prize_text = sos.get('prize')
            if other_mentions:
                await user.send(
                    f"You won a Split Or Steal for {prize_text} against {other_mentions}! Choose Split or Steal within 5 minutes.",
                    view=local_view,
                )
            else:
                await user.send(
                    f"You won a Split Or Steal for {prize_text}! Choose Split or Steal within 5 minutes.",
                    view=local_view,
                )
        except Exception:
            # Can't DM; treat as non-responsive
            pass
        except Exception:
            # Can't DM; treat as non-responsive
            pass

    # wait up to 5 minutes for both choices, but allow early finalize when both choose
    start = datetime.now(timezone.utc)
    timeout = 300
    while (datetime.now(timezone.utc) - start).total_seconds() < timeout:
        await asyncio.sleep(1)
        async with DATA_LOCK:
            # if choices reached 2 and someone already triggered finalize, break
            if len(sos.get('choices', {})) >= 2:
                break

    # finalize (may be called earlier by ChoiceView)
    await finalize_sos(sid)


async def handle_dbd_end(sid: str, delay: int):
    await asyncio.sleep(delay)
    async with DATA_LOCK:
        dbd_game = bot.active_dbd.get(sid)
        if not dbd_game:
            return
        # keep active_dbd until resolved
    # choose initial winner
    entries = list(dbd_game.get('entries', []))
    channel = bot.get_channel(dbd_game.get('channel_id'))
    if not channel:
        return
    if not entries:
        await channel.send("Double Down ended — no entries.")
        # cleanup
        async with DATA_LOCK:
            bot.active_dbd.pop(sid, None)
        return
    winner = random.choice(entries)
    # start prompting winner
    await prompt_dbd_winner(sid, winner)


async def handle_rps_end(sid: str, delay: int):
    await asyncio.sleep(delay)
    async with DATA_LOCK:
        rps = bot.active_rps.get(sid)
        if not rps:
            return
        # remove from giveaway tracker but keep rps active for choices
        bot.active_giveaways.pop(sid, None)
    if not rps:
        return
    entries = list(rps['entries'])
    channel = bot.get_channel(rps['channel_id'])
    if not channel:
        return
    try:
        message = await channel.fetch_message(rps['message_id'])
    except Exception:
        message = None
    winners = []
    if len(entries) >= 2:
        winners = random.sample(entries, 2)
    elif len(entries) == 1:
        winners = entries
    embed = discord.Embed(title="Rock Paper Scissors Ended", description=f"Prize: {rps['prize']}")
    embed.add_field(name="Entries", value=str(len(entries)))
    if not winners or len(winners) < 2:
        embed.add_field(name="Result", value="Not enough participants — no game")
        if message:
            await message.edit(embed=embed, view=None)
        else:
            await channel.send(embed=embed)
        return
    rps['winners'] = winners
    # announce the duel in-channel
    try:
        emoji = random.choice(EMOJI_POOL)
        await channel.send(f"{emoji} <@{winners[0]}> is dueling <@{winners[1]}> right now for {rps.get('prize')} {emoji}")
    except Exception:
        pass
    # DM both winners with choice buttons
    for uid in winners:
        user = bot.get_user(uid)
        if not user:
            continue
        view = RpsChoiceView(sid, uid, bot)
        try:
            await user.send(f"You are a winner in Rock Paper Scissors! Reply within 5 minutes. Choose Rock, Paper, or Scissors.", view=view)
        except Exception:
            # Can't DM; treat as non-responsive
            pass

    # wait up to 5 minutes for both choices, allow early finalize when both choose
    start = datetime.now(timezone.utc)
    timeout = 300
    while (datetime.now(timezone.utc) - start).total_seconds() < timeout:
        await asyncio.sleep(1)
        async with DATA_LOCK:
            if len(rps.get('choices', {})) >= 2:
                break

    await finalize_rps(sid)


async def handle_memory_end(sid: str, delay: int):
    await asyncio.sleep(delay)
    async with DATA_LOCK:
        mem = bot.active_memory.get(sid)
        if not mem:
            return
        # remove from giveaway tracker but keep memory active for submission
        bot.active_giveaways.pop(sid, None)
    if not mem:
        return
    entries = list(mem['entries'])
    channel = bot.get_channel(mem['channel_id'])
    if not channel:
        return
    try:
        message = await channel.fetch_message(mem['message_id'])
    except Exception:
        message = None
    if not entries:
        embed = discord.Embed(title="Memory Ended", description=f"Prize: {mem['prize']}")
        embed.add_field(name="Entries", value="0")
        embed.add_field(name="Result", value="No entries — game cancelled")
        if message:
            await message.edit(embed=embed, view=None)
        else:
            await channel.send(embed=embed)
        async with DATA_LOCK:
            bot.active_memory.pop(sid, None)
        return
    # pick one winner
    winner = random.choice(entries)
    async with DATA_LOCK:
        mem = bot.active_memory.get(sid)
        if not mem:
            return
        mem['winner'] = winner
        bot.active_memory[sid] = mem
    # announce and ping the winner, then display sequence one emoji per second
    try:
        emoji = random.choice(EMOJI_POOL)
        await channel.send(f"{emoji} Congratulations <@{winner}> — you're the contestant for {mem.get('prize')}! Watch the sequence below.")
    except Exception:
        pass
    try:
        countdown_msg = await channel.send(f"<@{winner}>, sequence is starting in 5 seconds!")
    except Exception:
        countdown_msg = None
    seq = mem.get('sequence', [])
    # wait a few seconds so the winner can read the announcement before the sequence starts
    for t in (4, 3, 2, 1):
        await asyncio.sleep(1)
        if countdown_msg:
            try:
                await countdown_msg.edit(content=f"<@{winner}>, sequence is starting in {t} seconds!")
            except Exception:
                pass
    await asyncio.sleep(1)
    try:
        seq_msg = await channel.send("Showing sequence:")
    except Exception:
        seq_msg = None
    # reveal one emoji per second by editing the message
    if seq_msg:
        shown = []
        for e in seq:
            shown.append(e)
            try:
                await seq_msg.edit(content=" ".join(shown))
            except Exception:
                pass
            await asyncio.sleep(1)
        # send a separate countdown message 3 seconds before deleting
        try:
            countdown_msg = await channel.send("Deleting in 3...")
        except Exception:
            countdown_msg = None
        for t in (2, 1):
            await asyncio.sleep(1)
            if countdown_msg:
                try:
                    await countdown_msg.edit(content=f"Deleting in {t}...")
                except Exception:
                    pass
        # final 1-second wait then delete both messages
        await asyncio.sleep(1)
        try:
            if seq_msg:
                await seq_msg.delete()
        except Exception:
            pass
        try:
            if countdown_msg:
                await countdown_msg.delete()
        except Exception:
            pass
    try:
        await channel.send(f"<@{winner}>, sequence ended — please check your DMs to enter the sequence you saw.")
    except Exception:
        pass

    # DM the winner and wait for their typed reply in DMs
    try:
        user = bot.get_user(winner)
        if not user:
            user = await bot.fetch_user(winner)
        if user:
            try:
                await user.send(
                    f"You were chosen for the Memory challenge for {mem.get('prize')}!\n\n"
                    f"Watch the sequence shown in the channel and then reply to me in this DM with the sequence in the SAME ORDER you saw it. You have 5 minutes to respond.",
                )
            except Exception:
                await channel.send(f"Could not DM <@{winner}>. Memory game aborted.")
                async with DATA_LOCK:
                    bot.active_memory.pop(sid, None)
                return

            def _check(m: discord.Message):
                return m.author.id == winner and isinstance(m.channel, discord.DMChannel)

            attempts = 3
            while attempts > 0:
                try:
                    msg = await bot.wait_for('message', timeout=300.0, check=_check)
                except asyncio.TimeoutError:
                    await channel.send(f"<@{winner}> did not respond in time — no winner for {mem.get('prize')}.")
                    async with DATA_LOCK:
                        bot.active_memory.pop(sid, None)
                    return

                answer = msg.content.strip()

                # ask for confirmation via DM buttons
                confirm_view = MemoryConfirmView(sid, winner, answer)
                try:
                    await user.send(f"Lock in this answer? {answer}", view=confirm_view)
                except Exception:
                    await channel.send(f"Could not send confirmation DM to <@{winner}>. Memory game aborted.")
                    async with DATA_LOCK:
                        bot.active_memory.pop(sid, None)
                    return

                # wait for user's confirmation choice
                await confirm_view.wait()
                if confirm_view.confirmed is True:
                    norm_ans = ''.join(answer.split())
                    norm_expected = ''.join(seq)
                    correct = norm_ans == norm_expected
                    display_correct = " ".join(seq)
                    if correct:
                        try:
                            await channel.send(f"<@{winner}> submitted: {answer} — correct! WON {mem.get('prize')}!")
                        except Exception:
                            pass
                        await award_credits_for_prize(bot, [winner], mem.get('prize'))
                    else:
                        try:
                            await channel.send(f"<@{winner}> responded with: {answer} — but the correct sequence is: {display_correct}. They did not win {mem.get('prize')}.")
                        except Exception:
                            pass
                    async with DATA_LOCK:
                        bot.active_memory.pop(sid, None)
                    return
                elif confirm_view.confirmed is False:
                    # allow redo
                    attempts -= 1
                    try:
                        await user.send("Okay — reply again in this DM with your sequence.")
                    except Exception:
                        await channel.send(f"Could not DM <@{winner}> for redo. Memory game aborted.")
                        async with DATA_LOCK:
                            bot.active_memory.pop(sid, None)
                        return
                    continue
                else:
                    # no confirmation choice (timeout)
                    try:
                        await user.send("Confirmation timed out — memory attempt cancelled.")
                    except Exception:
                        pass
                    await channel.send(f"<@{winner}> did not confirm their answer in time — no winner for {mem.get('prize')}.")
                    async with DATA_LOCK:
                        bot.active_memory.pop(sid, None)
                    return
    except Exception:
        logging.exception("Error DMing memory winner or waiting for response")


async def handle_luckynumber_end(sid: str, delay: int):
    await asyncio.sleep(delay)
    async with DATA_LOCK:
        game = bot.active_luckynumber.pop(sid, None)
    if not game:
        return
    channel = bot.get_channel(game.get('channel_id'))
    if not channel:
        return
    message = None
    if game.get('message_id'):
        try:
            message = await channel.fetch_message(game.get('message_id'))
        except Exception:
            message = None

    prize = game.get('prize')
    guesses_count = game.get('guesses', 0)
    target = game.get('target')
    embed = discord.Embed(title="Lucky Number Ended", description=f"Prize: {prize}")
    embed.add_field(name="Number", value=str(target), inline=True)
    embed.add_field(name="Guesses", value=str(guesses_count), inline=True)
    embed.add_field(name="Result", value="No one guessed the number in time.", inline=False)
    if message:
        try:
            await message.edit(embed=embed, view=None)
        except Exception:
            await channel.send(embed=embed)
    else:
        await channel.send(embed=embed)


async def finalize_luckynumber(sid: str, winner_id: int, guess: int):
    async with DATA_LOCK:
        game = bot.active_luckynumber.pop(sid, None)
    if not game:
        return
    channel = bot.get_channel(game.get('channel_id'))
    if not channel:
        return
    message = None
    if game.get('message_id'):
        try:
            message = await channel.fetch_message(game.get('message_id'))
        except Exception:
            message = None

    prize = game.get('prize')
    guesses_count = game.get('guesses', 0)
    target = game.get('target')
    embed = discord.Embed(title="Lucky Number Won", description=f"Prize: {prize}")
    embed.add_field(name="Winner", value=f"<@{winner_id}>", inline=True)
    embed.add_field(name="Number", value=str(target), inline=True)
    embed.add_field(name="Guesses", value=str(guesses_count), inline=True)
    if message:
        try:
            await message.edit(embed=embed, view=None)
        except Exception:
            await channel.send(embed=embed)
    else:
        await channel.send(embed=embed)
    emoji = random.choice(EMOJI_POOL)
    try:
        await channel.send(f"{emoji} <@{winner_id}> guessed **{guess}** and WON {prize}! {emoji}")
    except Exception:
        pass
    await award_credits_for_prize(bot, [winner_id], prize)


async def handle_trivia_end(sid: str, delay: int):
    await asyncio.sleep(delay)
    async with DATA_LOCK:
        game = bot.active_trivia.get(sid)
        if not game:
            return
        bot.active_trivia[sid] = game
    entries = list(game.get('entries', []))
    channel = bot.get_channel(game.get('channel_id'))
    if not channel:
        return
    message = None
    if game.get('message_id'):
        try:
            message = await channel.fetch_message(game.get('message_id'))
        except Exception:
            message = None
    if not entries:
        embed = discord.Embed(title="Trivia Ended", description=f"Prize: {game.get('prize')}")
        embed.add_field(name="Result", value="No entries — game cancelled", inline=False)
        if message:
            await message.edit(embed=embed, view=None)
        else:
            await channel.send(embed=embed)
        async with DATA_LOCK:
            bot.active_trivia.pop(sid, None)
        return

    # pick a random winner to answer
    winner_id = random.choice(entries)
    question = game.get('question')
    answer = str(game.get('answer', '')).strip()
    prize = game.get('prize')

    # announce selection in channel
    try:
        await channel.send(
            f"🎯 <@{winner_id}> has been selected for Trivia! "
            f"Check your DMs and answer within 60 seconds."
        )
    except Exception:
        pass

    # DM the winner
    user = bot.get_user(winner_id)
    if not user:
        try:
            user = await bot.fetch_user(winner_id)
        except Exception:
            user = None
    if not user:
        await channel.send("Trivia ended — could not contact the chosen winner.")
        async with DATA_LOCK:
            bot.active_trivia.pop(sid, None)
        return

    try:
        await user.send(
            f"You were selected for Trivia! Answer this within 60 seconds:\n"
            f"**Question:** {question}\n"
            f"Reply with just your answer."
        )
    except Exception:
        await channel.send(f"Trivia ended — could not DM <@{winner_id}>.")
        async with DATA_LOCK:
            bot.active_trivia.pop(sid, None)
        return

    def check(msg: discord.Message):
        return msg.author.id == winner_id and isinstance(msg.channel, discord.DMChannel)

    response_text = None
    correct = False
    deadline = datetime.now(timezone.utc) + timedelta(seconds=60)
    while True:
        remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            response_text = None
            correct = False
            break
        try:
            msg = await bot.wait_for("message", timeout=remaining, check=check)
        except asyncio.TimeoutError:
            response_text = None
            correct = False
            break

        candidate = msg.content.strip()
        # confirm answer
        try:
            view = TriviaConfirmView(winner_id)
            await msg.channel.send(f'Lock in this answer?\n"{candidate}"', view=view)
            await view.wait()
        except Exception:
            view = None

        if not view or view.confirmed is True:
            response_text = candidate
            correct = response_text.lower() == answer.lower()
            break
        if view.confirmed is False:
            try:
                await msg.channel.send("Okay — reply again with your final answer.")
            except Exception:
                pass
            continue

    # announce the question and their response right after they reply (green if correct, red if not)
    if response_text is not None:
        try:
            outcome = f"Correct! They win {prize}." if correct else "Incorrect — no winner."
            if not correct and answer:
                outcome = f"Incorrect — correct answer: {answer}."
            color = discord.Color.green() if correct else discord.Color.red()
            try:
                await channel.send(f"<@{winner_id}> — your response is in!")
            except Exception:
                pass
            quick = discord.Embed(title="Trivia Response", color=color)
            quick.add_field(name="Winner", value=f"<@{winner_id}>", inline=True)
            quick.add_field(name="Question", value=question, inline=False)
            quick.add_field(name="Answer", value=response_text, inline=False)
            quick.add_field(name="Outcome", value=outcome, inline=False)
            await channel.send(embed=quick)
        except Exception:
            pass

    # announce in channel (summary only)
    entries_count = len(entries)
    result_embed = discord.Embed(title="Trivia Ended", description=f"Prize: {prize}")
    result_embed.add_field(name="Winner", value=f"<@{winner_id}>", inline=True)
    result_embed.add_field(name="Entries", value=str(entries_count), inline=True)
    if response_text is None:
        result_embed.add_field(name="Result", value="No response in time — no winner.", inline=False)
    elif correct:
        result_embed.add_field(name="Result", value=f"Correct — <@{winner_id}> wins {prize}!", inline=False)
    else:
        result_embed.add_field(name="Result", value="Incorrect — no winner.", inline=False)

    if message:
        try:
            await message.edit(embed=result_embed, view=None)
        except Exception:
            await channel.send(embed=result_embed)
    else:
        await channel.send(embed=result_embed)

    if correct:
        await award_credits_for_prize(bot, [winner_id], prize)

    async with DATA_LOCK:
        bot.active_trivia.pop(sid, None)


async def handle_don_end(sid: str, delay: int):
    await asyncio.sleep(delay)
    async with DATA_LOCK:
        game = bot.active_don.get(sid)
        if not game:
            return
    entries = list(game.get('entries', []))
    channel = bot.get_channel(game.get('channel_id'))
    if not channel:
        return
    message = None
    if game.get('message_id'):
        try:
            message = await channel.fetch_message(game.get('message_id'))
        except Exception:
            message = None

    if not entries:
        embed = discord.Embed(title="Double or Nothing Ended", description=f"Prize: {game.get('prize')}")
        embed.add_field(name="Result", value="No entries — game cancelled", inline=False)
        if message:
            await message.edit(embed=embed, view=None)
        else:
            await channel.send(embed=embed)
        async with DATA_LOCK:
            bot.active_don.pop(sid, None)
        return

    winner = random.choice(entries)
    async with DATA_LOCK:
        game = bot.active_don.get(sid)
        if not game:
            return
        game['winner'] = winner
        bot.active_don[sid] = game

    # update embed to show winner selected
    try:
        embed = discord.Embed(title="Double or Nothing Ended", description=f"Prize: {game.get('prize')}")
        embed.add_field(name="Winner", value=f"<@{winner}>", inline=True)
        embed.add_field(name="Entries", value=str(len(entries)), inline=True)
        embed.add_field(name="Status", value="Winner selected — check DMs to choose.", inline=False)
        if message:
            await message.edit(embed=embed, view=None)
        else:
            await channel.send(embed=embed)
    except Exception:
        pass

    # announce and DM the winner
    try:
        emoji = random.choice(EMOJI_POOL)
        await channel.send(f"{emoji} <@{winner}> — you're up! Check your DMs to choose Keep or Double.")
    except Exception:
        pass

    user = bot.get_user(winner)
    if not user:
        try:
            user = await bot.fetch_user(winner)
        except Exception:
            user = None
    if not user:
        await channel.send("Could not DM the winner. Double or Nothing ended.")
        async with DATA_LOCK:
            bot.active_don.pop(sid, None)
        return
    try:
        prize_text = format_prize_with_multiplier(game.get('base_prize'), game.get('multiplier', 1))
        view = DonChoiceView(sid, winner, bot)
        await user.send(
            f"You won {prize_text}! Choose **Keep** to secure it, or **Double (50/50)** to try for double.",
            view=view,
        )
        async with DATA_LOCK:
            game = bot.active_don.get(sid)
            if game:
                game['awaiting_choice'] = True
                bot.active_don[sid] = game
    except Exception:
        await channel.send(f"Could not DM <@{winner}>. Double or Nothing ended.")
        async with DATA_LOCK:
            bot.active_don.pop(sid, None)
        return


async def process_don_choice(botref, sid: str, uid: int, choice: str):
    async with DATA_LOCK:
        game = botref.active_don.get(sid)
        if not game:
            return
        if game.get('finalized'):
            return
        if not game.get('awaiting_choice'):
            return
        game['awaiting_choice'] = False
        botref.active_don[sid] = game
        channel = botref.get_channel(game.get('channel_id'))
        message_id = game.get('message_id')
        base_prize = game.get('base_prize', game.get('prize'))
        multiplier = int(game.get('multiplier', 1))
        risk_mode = game.get('risk_mode', 'coin')
        prize = format_prize_with_multiplier(base_prize, multiplier)

    result_embed = discord.Embed(title="Double or Nothing Result", description=f"Prize: {prize}")
    result_embed.add_field(name="Winner", value=f"<@{uid}>", inline=True)
    result_embed.add_field(name="Choice", value=choice.title(), inline=True)

    if choice == "keep":
        outcome_text = f"<@{uid}> kept the prize: {prize}."
        result_embed.add_field(name="Outcome", value=outcome_text, inline=False)
        async with DATA_LOCK:
            game = botref.active_don.get(sid)
            if game:
                game['finalized'] = True
                botref.active_don[sid] = game
        await award_credits_for_prize(botref, [uid], base_prize, multiplier=multiplier)
    else:
        # visible risk animation in-channel
        if channel:
            try:
                if risk_mode == "coin":
                    await channel.send(f"<@{uid}> chose to risk and **double** it — coin flip is starting now.")
                    flip_msg = await channel.send("🪙 Flipping coin...")
                    for _ in range(3):
                        await asyncio.sleep(0.6)
                        await flip_msg.edit(content=f"🪙 Flipping coin... **{random.choice(['Heads','Tails'])}**")
                    await asyncio.sleep(0.6)
                    flip_face = random.choice(["Heads", "Tails"])
                    await flip_msg.edit(content=f"🪙 Coin landed on **{flip_face}**")
                else:
                    if risk_mode == "roulette":
                        await channel.send(f"<@{uid}> chose to risk and **double** it — roulette is spinning now.")
                        roll_msg = await channel.send("🎯 Spinning chambers...")
                        for _ in range(4):
                            await asyncio.sleep(0.5)
                            await roll_msg.edit(content=f"🎯 Spinning chambers... **{random.randint(1,5)}/5**")
                        await asyncio.sleep(0.6)
                        chamber = random.randint(1,5)
                        await roll_msg.edit(content=f"🎯 Chamber landed on **{chamber}/5**")
                    else:
                        await channel.send(f"<@{uid}> chose to risk and **double** it — wheel is spinning now.")
                        wheel_msg = await channel.send("🎡 Spinning the Wheel of Fate...")
                        for _ in range(4):
                            await asyncio.sleep(0.5)
                            await wheel_msg.edit(content=f"🎡 Wheel of Fate... **{random.choice(['50%','33%','25%','20%'])}**")
                        await asyncio.sleep(0.6)
                        await wheel_msg.edit(content="🎡 Wheel of Fate stopped.")
            except Exception:
                pass

        if risk_mode == "coin":
            win = (flip_face == "Heads")
            display_roll = f"Coin: {flip_face}"
            win_multiplier = 2
            odds_text = "50%"
        elif risk_mode == "roulette":
            win = (chamber == 1)
            display_roll = f"Chamber: {chamber}/5"
            win_multiplier = 5
            odds_text = "20%"
        else:
            # wheel of fate: random odds/multiplier each spin
            wheel_options = [
                (0.5, 2, "50%"),
                (0.33, 3, "33%"),
                (0.25, 4, "25%"),
                (0.2, 5, "20%"),
            ]
            chance, win_multiplier, odds_text = random.choice(wheel_options)
            win = random.random() < chance
            display_roll = f"Wheel: {odds_text}"

        if win:
            new_multiplier = multiplier * win_multiplier
            doubled = format_prize_with_multiplier(base_prize, new_multiplier)
            if risk_mode == "coin":
                outcome_text = f"Coin flip {flip_face} — WIN! <@{uid}> doubles to {doubled}! You can choose to keep or double again."
            elif risk_mode == "roulette":
                outcome_text = f"Roulette {display_roll} — WIN! <@{uid}> boosts to {doubled}! You can choose to keep or double again."
            else:
                outcome_text = f"Wheel of Fate {display_roll} — WIN! <@{uid}> boosts to {doubled}! You can choose to keep or double again."
            result_embed.add_field(name="Risk Roll", value=display_roll, inline=True)
            result_embed.add_field(name="Odds", value=odds_text, inline=True)
            result_embed.add_field(name="Outcome", value=outcome_text, inline=False)
            # allow another decision
            async with DATA_LOCK:
                game = botref.active_don.get(sid)
                if not game:
                    return
                game['multiplier'] = new_multiplier
                game['awaiting_choice'] = True
                botref.active_don[sid] = game
            # DM again for next choice
            try:
                user = botref.get_user(uid) or await botref.fetch_user(uid)
                if user:
                    view = DonChoiceView(sid, uid, botref)
                    if risk_mode == "coin":
                        await user.send(
                            f"You won the coin flip! Your prize is now {doubled}. "
                            f"Choose **Keep** to secure it, or **Double (50/50)** to risk it again.",
                            view=view,
                        )
                    elif risk_mode == "roulette":
                        await user.send(
                            f"You won the roulette! Your prize is now {doubled}. "
                            f"Choose **Keep** to secure it, or **Double (20%)** to risk it again.",
                            view=view,
                        )
                    else:
                        await user.send(
                            f"You won the Wheel of Fate! Your prize is now {doubled}. "
                            f"Choose **Keep** to secure it, or **Double (??%)** to risk it again.",
                            view=view,
                        )
            except Exception:
                pass
        else:
            if risk_mode == "coin":
                outcome_text = f"Coin flip {flip_face} — LOSS. <@{uid}> gets nothing."
            elif risk_mode == "roulette":
                outcome_text = f"Roulette {display_roll} — LOSS. <@{uid}> gets nothing."
            else:
                outcome_text = f"Wheel of Fate {display_roll} — LOSS. <@{uid}> gets nothing."
            result_embed.add_field(name="Risk Roll", value=display_roll, inline=True)
            result_embed.add_field(name="Odds", value=odds_text, inline=True)
            result_embed.add_field(name="Outcome", value=outcome_text, inline=False)
            async with DATA_LOCK:
                game = botref.active_don.get(sid)
                if game:
                    game['finalized'] = True
                    botref.active_don[sid] = game

    # edit original message if possible, otherwise post a new one
    if channel:
        if message_id:
            try:
                msg = await channel.fetch_message(message_id)
            except Exception:
                msg = None
            if msg:
                try:
                    await msg.edit(embed=result_embed, view=None)
                except Exception:
                    await channel.send(embed=result_embed)
            else:
                await channel.send(embed=result_embed)
        else:
            await channel.send(embed=result_embed)
        # announce result in-channel (always ping the winner)
        try:
            await channel.send(outcome_text)
        except Exception:
            pass

    async with DATA_LOCK:
        game = botref.active_don.get(sid)
        if game and game.get('finalized'):
            botref.active_don.pop(sid, None)


async def handle_maze_end(sid: str, delay: int):
    await asyncio.sleep(delay)
    async with DATA_LOCK:
        maze = bot.active_maze.get(sid)
        if not maze:
            return
        bot.active_giveaways.pop(sid, None)
    if not maze:
        return
    entries = list(maze['entries'])
    channel = bot.get_channel(maze['channel_id'])
    if not channel:
        return
    try:
        message = await channel.fetch_message(maze['message_id'])
    except Exception:
        message = None
    if not entries:
        embed = discord.Embed(title="Maze Ended", description=f"Prize: {maze['prize']}")
        embed.add_field(name="Entries", value="0")
        embed.add_field(name="Result", value="No entries — game cancelled")
        if message:
            await message.edit(embed=embed, view=None)
        else:
            await channel.send(embed=embed)
        async with DATA_LOCK:
            bot.active_maze.pop(sid, None)
        return
    # choose a winner and begin DM sequence steps
    winner = random.choice(entries)
    async with DATA_LOCK:
        maze = bot.active_maze.get(sid)
        if not maze:
            return
        maze['winner'] = winner
        maze['index'] = 0
        bot.active_maze[sid] = maze
    # announce and ping the winner
    try:
        emoji = random.choice(EMOJI_POOL)
        await channel.send(f"{emoji} <@{winner}> — you were selected to navigate the maze for {maze.get('prize')}. Check your DMs to start.")
    except Exception:
        pass
    # DM the winner with MazeChoiceView
    try:
        user = bot.get_user(winner)
        if not user:
            user = await bot.fetch_user(winner)
        view = MazeChoiceView(sid, winner, maze.get('sequence', []), bot)
        try:
            await user.send(
                f"Navigate the maze: you will be presented with {len(maze.get('sequence', []))} steps. Choose Left/Middle/Right. You have 5 minutes total.",
                view=view,
            )
        except Exception:
            # can't DM winner — try other candidates
            async with DATA_LOCK:
                maze = bot.active_maze.get(sid)
                if not maze:
                    return
                entries2 = list(maze.get('entries', []))
                try:
                    entries2.remove(winner)
                except ValueError:
                    pass
                maze['entries'] = set(entries2)
                bot.active_maze[sid] = maze
            if entries2:
                # pick another candidate synchronously
                await handle_maze_end(sid, 0)
            else:
                await channel.send("Maze aborted — no reachable winner to DM.")
                async with DATA_LOCK:
                    bot.active_maze.pop(sid, None)
            return
    except Exception:
        pass


async def handle_reactroulette_end(sid: str, delay: int):
    await asyncio.sleep(delay)
    async with DATA_LOCK:
        rr = bot.active_reactroulette.pop(sid, None)
    if not rr:
        return
    channel = bot.get_channel(rr.get('channel_id'))
    if not channel:
        return
    try:
        msg = await channel.fetch_message(rr.get('message_id'))
    except Exception:
        msg = None
    options = rr.get('options', [])
    if not options:
        return
    # pick 3 emojis for the actual roulette (supports option counts > 3)
    try:
        play_options = random.sample(options, k=min(3, len(options)))
    except Exception:
        play_options = options[:3]
    entries = list(rr.get('entries', []))
    winner = None
    if entries:
        winner = random.choice(entries)
    else:
        # fallback: pick from anyone who reacted to the message
        reactors = []
        try:
            if msg:
                for react in msg.reactions:
                    users = [u async for u in react.users()]
                    users = [u for u in users if u.id != bot.user.id]
                    reactors.extend([u.id for u in users])
        except Exception:
            reactors = []
        reactors = list(dict.fromkeys(reactors))
        if reactors:
            winner = random.choice(reactors)

    emoji = random.choice(EMOJI_POOL)
    if not winner:
        try:
            await channel.send(f"{emoji} Reaction Roulette ended — no entries, no winner.")
        except Exception:
            pass
        return

    try:
        await channel.send(f"{emoji} Reaction Roulette started — <@{winner}> was selected! Check your DMs to pick an emoji.")
    except Exception:
        pass

    # DM the winner with 3 emoji buttons
    try:
        user = bot.get_user(winner)
        if not user:
            user = await bot.fetch_user(winner)
        if not user:
            raise Exception("User not found")
        pick_view = ReactRouletteChoiceView(sid, winner, bot, play_options)
        opts_display = ' '.join(play_options)
        await user.send(
            f"You were selected for Reaction Roulette for {rr.get('prize')}!\n"
            f"Pick one of these emojis: {opts_display}",
            view=pick_view
        )
    except Exception:
        try:
            await channel.send(f"{emoji} Could not DM <@{winner}> — no winner.")
        except Exception:
            pass
        return

    # wait for the winner's pick
    await pick_view.wait()
    if not pick_view.chosen:
        try:
            await channel.send(f"{emoji} <@{winner}> did not choose in time — no winner.")
        except Exception:
            pass
        return

    chosen = pick_view.chosen
    # run roulette animation in channel
    try:
        roulette_msg = await channel.send(
            f"🎰 Roulette for <@{winner}>\n"
            f"`{' '.join(play_options)}`\n"
            f"`{' '.join(['🔴' for _ in play_options])}`"
        )
    except Exception:
        roulette_msg = None

    final_index = random.randint(0, len(play_options) - 1)
    steps = random.randint(12, 18)
    for i in range(steps):
        idx = i % len(play_options)
        if i == steps - 1:
            idx = final_index
        lights = ["🔴" for _ in play_options]
        lights[idx] = "🟢"
        if roulette_msg:
            try:
                roulette_content = (
                    f"🎰 Roulette for <@{winner}>\n"
                    f"`{' '.join(play_options)}`\n"
                    f"`{' '.join(lights)}`"
                )
                await roulette_msg.edit(content=roulette_content)
            except Exception:
                pass
        await asyncio.sleep(0.6)

    landed = play_options[final_index]
    if chosen == landed:
        try:
            await channel.send(f"{emoji} <@{winner}> picked {chosen} — roulette landed on {landed}. They WON {rr.get('prize')}! {emoji}")
        except Exception:
            pass
        await award_credits_for_prize(bot, [winner], rr.get('prize'))
    else:
        try:
            await channel.send(f"{emoji} <@{winner}> picked {chosen} — roulette landed on {landed}. They did not win {rr.get('prize')}.")
        except Exception:
            pass


@bot.tree.command(name="reactroulette")
async def reactroulette(interaction: discord.Interaction):
    """Create a reaction roulette: bot posts a set of emoji, users react, bot picks a winning emoji and earliest reactor wins."""
    class RRModal(discord.ui.Modal, title="Create Reaction Roulette"):
        duration = discord.ui.TextInput(label="Duration (e.g. 30s, 1m)", style=discord.TextStyle.short, placeholder="30s", required=True)
        prize = discord.ui.TextInput(label="Prize description", style=discord.TextStyle.long, placeholder="Describe the prize", required=True)
        options = discord.ui.TextInput(label="Options (comma-separated emoji count)", style=discord.TextStyle.short, placeholder="3", required=False)
        allowed_role = discord.ui.TextInput(
            label="Required role (optional)",
            style=discord.TextStyle.short,
            placeholder="Role mention or ID (leave blank for none)",
            required=False,
            max_length=64,
        )

        async def on_submit(self, modal_interaction: discord.Interaction):
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
            role_id = None
            role_input = self.allowed_role.value.strip()
            if role_input:
                role, err = resolve_role_from_input(role_input, modal_interaction.guild)
                if err:
                    await modal_interaction.response.send_message(err, ephemeral=True)
                    return
                role_id = role.id if role else None
            try:
                opt_count = int(self.options.value.strip() or 3)
                opt_count = max(2, min(6, opt_count))
            except Exception:
                opt_count = 3
            sid = uuid.uuid4().hex[:8]
            # pick distinct emojis
            opts = random.sample(EMOJI_POOL, k=opt_count)
            rr = {
                'id': sid,
                'creator': modal_interaction.user.id,
                'end_time': (datetime.now(timezone.utc) + timedelta(seconds=seconds)).timestamp(),
                'prize': prize_text,
                'channel_id': modal_interaction.channel.id if modal_interaction.channel else None,
                'message_id': None,
                'options': opts,
                'first_reactors': {},
                'entries': set(),
                'allowed_role_id': role_id,
            }
            if rr['channel_id'] is None:
                await modal_interaction.response.send_message("Cannot determine channel to post.", ephemeral=True)
                return
            channel = bot.get_channel(rr['channel_id'])
            if channel is None:
                await modal_interaction.response.send_message("Channel not found.", ephemeral=True)
                return
            opts_display = ' '.join(opts)
            embed = discord.Embed(title=prize_text, description="Reaction Roulette")
            embed.add_field(name="Ends", value=f"<t:{int(rr['end_time'])}:R>", inline=True)
            embed.add_field(name="Entries", value="0", inline=True)
            embed.add_field(name="Host", value=f"<@{modal_interaction.user.id}>", inline=False)
            view = JoinView(sid, bot)
            msg = await channel.send(embed=embed, view=view)
            rr['message_id'] = msg.id
            bot.active_reactroulette[sid] = rr
            asyncio.create_task(handle_reactroulette_end(sid, seconds))
            await modal_interaction.response.send_message(f"Reaction Roulette started with options: {opts_display}", ephemeral=True)

    await interaction.response.send_modal(RRModal())
    


async def prompt_dbd_winner(sid: str, winner_id: int):
    async with DATA_LOCK:
        dbd = bot.active_dbd.get(sid)
        if not dbd:
            return
        prize = dbd.get('prize')
        channel = bot.get_channel(dbd.get('channel_id'))
    # announce in channel that winner is choosing
    try:
        await channel.send(f"<@{winner_id}> is choosing to keep or double...")
    except Exception:
        pass
    # DM the winner with choice buttons
    view = DbdChoiceView(sid, winner_id, bot)
    # build display prize with multiplier
    mult = dbd.get('multiplier', 1)
    base = dbd.get('base_prize', prize)
    display = format_prize_with_multiplier(base, mult)
    try:
        user = bot.get_user(winner_id)
        if not user:
            try:
                user = await bot.fetch_user(winner_id)
            except Exception:
                user = None
        if not user:
            raise Exception("User not found")
        await user.send(f"You won {display}! Do you want to Keep or Double?", view=view)
    except Exception:
        # could not DM — try next candidate (remove this winner)
        async with DATA_LOCK:
            dbd = bot.active_dbd.get(sid)
            if not dbd:
                return
            entries = list(dbd.get('entries', []))
            try:
                entries.remove(winner_id)
            except ValueError:
                pass
            dbd['entries'] = set(entries)
            bot.active_dbd[sid] = dbd
        # choose another
        if entries:
            next_winner = random.choice(entries)
            await prompt_dbd_winner(sid, next_winner)
        else:
            await channel.send("Double Down ended — no reachable winners.")
            async with DATA_LOCK:
                bot.active_dbd.pop(sid, None)
        return


async def process_dbd_choice(botref, sid: str, uid: int, choice: str):
    async with DATA_LOCK:
        dbd = botref.active_dbd.get(sid)
        if not dbd:
            return
        channel = botref.get_channel(dbd.get('channel_id'))
        base = dbd.get('base_prize', dbd.get('prize'))
        mult = dbd.get('multiplier', 1)

    # handle keep
    if choice == 'keep':
        display = format_prize_with_multiplier(base, mult)
        # announce keep
        try:
            await channel.send(f"<@{uid}> chose to KEEP the prize: {display}")
        except Exception:
            pass
        await award_credits_for_prize(botref, [uid], base, multiplier=mult)
        # DM winner confirmation
        try:
            user = botref.get_user(uid)
            if not user:
                user = await botref.fetch_user(uid)
            await user.send(f"You kept the prize: {display}")
        except Exception:
            pass
        # cleanup
        async with DATA_LOCK:
            botref.active_dbd.pop(sid, None)
        return

    # handle double
    if choice == 'double':
        # double the multiplier (×2)
        mult = mult * 2
        # persist multiplier and get entries under lock
        async with DATA_LOCK:
            dbd = botref.active_dbd.get(sid)
            if not dbd:
                return
            dbd['multiplier'] = mult
            entries = list(dbd.get('entries', []))
        remaining = [e for e in entries if e != uid]
        if not remaining:
            # no one to double to, announce and give back to original
            display = format_prize_with_multiplier(base, mult)
            try:
                await channel.send(f"<@{uid}> tried to DOUBLE but no other players available. They keep the prize: {display}")
            except Exception:
                pass
            await award_credits_for_prize(botref, [uid], base, multiplier=mult)
            try:
                user = botref.get_user(uid)
                await user.send(f"No other players available to double. You keep the prize: {display}")
            except Exception:
                pass
            async with DATA_LOCK:
                botref.active_dbd.pop(sid, None)
            return
        new_winner = random.choice(remaining)
        # update winner under lock
        async with DATA_LOCK:
            dbd = botref.active_dbd.get(sid)
            if not dbd:
                return
            dbd['winner'] = new_winner
        display = format_prize_with_multiplier(base, mult)
        try:
            await channel.send(f"<@{uid}> chose to DOUBLE. New contender is <@{new_winner}> for prize: {display}")
        except Exception:
            pass
        # DM new winner
        try:
            user = botref.get_user(new_winner)
            if not user:
                user = await botref.fetch_user(new_winner)
            view = DbdChoiceView(sid, new_winner, botref)
            await user.send(f"You are the new contender for {display}. Keep or Double?", view=view)
        except Exception:
            pass
        return


async def finalize_sos(sid: str):
    async with DATA_LOCK:
        sos = bot.active_sos.get(sid)
        if not sos:
            return
        # prevent double-finalize if called twice
        if sos.get('finalized'):
            return
        sos['finalized'] = True
        bot.active_sos[sid] = sos
        winners = list(sos.get('winners', []))
        channel = bot.get_channel(sos.get('channel_id'))
        message = None
        msg_id = sos.get('message_id')
    # try to fetch the originating message (optional)
    if channel and msg_id:
        try:
            message = await channel.fetch_message(msg_id)
        except Exception:
            message = None

    async with DATA_LOCK:
        sos = bot.active_sos.get(sid)
        if not sos:
            return
        choices = dict(sos.get('choices', {}))

    if len(choices) < 2:
        if channel:
            await channel.send("One or more winners didn't choose in time — no one wins.")
        if message:
            try:
                await message.edit(view=None)
            except Exception:
                pass
        # cleanup
        async with DATA_LOCK:
            bot.active_sos.pop(sid, None)
        return

    # Post result to channel with game outcome
    result_lines = []
    for uid in winners:
        ch = choices.get(uid, 'no response')
        # emphasize the player's choice
        result_lines.append(f"<@{uid}>: **{str(ch).upper()}**")

    def norm(c):
        return (c or '').strip().lower()

    c0 = norm(choices.get(winners[0]))
    c1 = norm(choices.get(winners[1]))
    prize = sos.get('prize', 'the prize')

    verdict = None
    quote = None
    if c0 == 'split' and c1 == 'split':
        # compute each player's share using suffix-aware division
        share = format_prize_divided(prize, 2)
        verdict = f"{f'<@{winners[0]}>'} {f'<@{winners[1]}>'} will equally share {prize}! Each receives {share}."
        quote = '"Mercy is expensive - you both paid it." - respect earned.'
        await award_credits_for_prize(bot, winners, prize, split=2)
    elif c0 == 'steal' and c1 == 'steal':
        verdict = "Both players chose steal — no one gets anything."
        quote = '"Greed is symmetric. Nobody wins." - outcome locked.'
    else:
        if c0 == 'steal' or c1 == 'steal':
            stealer = winners[0] if c0 == 'steal' else winners[1]
            verdict = f"Giveaway result: <@{stealer}> wins — they get {prize}!"
            # special flourish when one split and one steal
            quote = '"Trust is the first casualty." - cold game.'
            await award_credits_for_prize(bot, [stealer], prize)

    emoji = random.choice(EMOJI_POOL)
    # color: green when both split, red otherwise (any steal)
    embed_color = discord.Color.green() if (c0 == 'split' and c1 == 'split') else discord.Color.red()
    # build embed result
    embed = discord.Embed(title="Split Or Steal results", color=embed_color)
    contestants = ' and '.join(f"<@{w}>" for w in winners)
    # ping winners as a plain message before the embed
    try:
        if channel:
            mentions = ' '.join(f"<@{w}>" for w in winners)
            await channel.send(f"{mentions} — Results are in!")
    except Exception:
        pass

    embed.add_field(name="Prize", value=f"**{prize}**", inline=False)
    embed.add_field(name="Contestants", value=f"**{contestants}**", inline=False)
    embed.add_field(name="Choices", value="\n".join(result_lines), inline=False)
    if verdict:
        embed.add_field(name="Outcome", value=f"**{verdict}**", inline=False)
    if quote:
        embed.add_field(name="Quote", value=f"**{quote}**", inline=False)
    embed.set_footer(text=emoji)
    if channel:
        await channel.send(embed=embed)
    # cleanup
    async with DATA_LOCK:
        bot.active_sos.pop(sid, None)


async def finalize_rps(sid: str):
    async with DATA_LOCK:
        rps = bot.active_rps.get(sid)
        if not rps:
            return
        # prevent double-finalize if called twice
        if rps.get('finalized'):
            return
        rps['finalized'] = True
        bot.active_rps[sid] = rps
        winners = list(rps.get('winners', []))
        channel = bot.get_channel(rps.get('channel_id'))
        message = None
        msg_id = rps.get('message_id')
    if channel and msg_id:
        try:
            message = await channel.fetch_message(msg_id)
        except Exception:
            message = None

    async with DATA_LOCK:
        rps = bot.active_rps.get(sid)
        if not rps:
            return
        choices = dict(rps.get('choices', {}))

    if len(choices) < 2:
        # if exactly one chose, report who chose and that the other didn't respond
        if len(choices) == 1:
            chooser_id = next(iter(choices.keys()))
            chooser_choice = choices.get(chooser_id)
            if channel:
                await channel.send(f"<@{chooser_id}> chose {chooser_choice}. The other winner did not choose in time — no winner for {rps.get('prize')}.")
            if message:
                try:
                    await message.edit(view=None)
                except Exception:
                    pass
            async with DATA_LOCK:
                bot.active_rps.pop(sid, None)
            return
        else:
            if channel:
                await channel.send("One or more winners didn't choose in time — no one wins.")
            if message:
                try:
                    await message.edit(view=None)
                except Exception:
                    pass
            async with DATA_LOCK:
                bot.active_rps.pop(sid, None)
            return

    # announce choices and determine winner
    a, b = winners[0], winners[1]
    ca = choices.get(a)
    cb = choices.get(b)
    # simple normalize
    def norm(c):
        return (c or '').strip().lower()

    ca_n = norm(ca)
    cb_n = norm(cb)
    prize = rps.get('prize', 'the prize')

    result_text = f"<@{a}> chose {ca_n} — <@{b}> chose {cb_n}."
    winner_mention = None
    # rules
    if ca_n == cb_n:
        result_text += f" It's a tie! No winner for {prize}."
    else:
        wins = {
            ('rock', 'scissors'),
            ('scissors', 'paper'),
            ('paper', 'rock'),
        }
        if (ca_n, cb_n) in wins:
            winner_mention = f"<@{a}>"
        elif (cb_n, ca_n) in wins:
            winner_mention = f"<@{b}>"
        if winner_mention:
            result_text += f" {winner_mention} won {prize}!"
        else:
            result_text += f" Unable to determine winner for {prize}."

    emoji = random.choice(EMOJI_POOL)
    if channel:
        await channel.send(f"{emoji} {result_text} {emoji}")

    # award credits to winner if any
    if winner_mention:
        winner_id = a if winner_mention == f"<@{a}>" else b
        await award_credits_for_prize(bot, [winner_id], prize)

    async with DATA_LOCK:
        bot.active_rps.pop(sid, None)


def update_auction_message(bot_ref, auc):
    """Fetch and update the auction message embed to reflect current highest bid."""
    async def _update():
        try:
            ch = bot_ref.get_channel(auc.get('channel_id'))
            if not ch:
                return
            if not auc.get('message_id'):
                return
            try:
                msg = await ch.fetch_message(auc.get('message_id'))
            except Exception:
                return
            highest = auc.get('highest')
            embed = discord.Embed(title="Auction", description=f"Item: {auc.get('prize')}")
            embed.add_field(name="Minimum Bid", value=f"{auc.get('min_bid'):,}")
            embed.add_field(name="Highest", value=f"{highest[1]:,}" if highest else "No bids")
            end_ts = int(auc.get('end_time', 0))
            if end_ts:
                embed.add_field(name="Ends", value=f"<t:{end_ts}:F> (<t:{end_ts}:R>)")
            try:
                await msg.edit(embed=embed, view=PlaceBidView(auc.get('id'), bot_ref))
            except Exception:
                try:
                    await msg.edit(embed=embed)
                except Exception:
                    pass
        except Exception:
            pass

    return asyncio.create_task(_update())


class PlaceBidView(discord.ui.View):
    def __init__(self, aid: str, bot_ref):
        super().__init__(timeout=None)
        self.aid = aid
        self.bot = bot_ref
        bid_btn = discord.ui.Button(label="Place a bid", style=discord.ButtonStyle.primary)

        async def bid_cb(interaction: discord.Interaction):
            async with DATA_LOCK:
                auction = self.bot.active_auctions.get(self.aid)
            if not auction:
                await interaction.response.send_message("This auction is no longer active.", ephemeral=True)
                return

            class BidModal(discord.ui.Modal, title="Place a Bid"):
                amount = discord.ui.TextInput(label="Your bid (e.g. 1k, 2.5m)", style=discord.TextStyle.short, required=True)

                async def on_submit(modal_self, modal_interaction: discord.Interaction):
                    val = modal_self.amount.value.strip()
                    try:
                        amt = parse_bid_amount(val)
                    except Exception as e:
                        await modal_interaction.response.send_message(f"Invalid bid: {e}", ephemeral=True)
                        return

                    async with DATA_LOCK:
                        auc = self.bot.active_auctions.get(self.aid)
                        if not auc:
                            await modal_interaction.response.send_message("Auction no longer active.", ephemeral=True)
                            return
                        min_bid = auc.get('min_bid', 0)
                        current = auc.get('highest')
                        current_amt = current[1] if current else 0
                        if amt < min_bid:
                            await modal_interaction.response.send_message(f"Bid must be at least {min_bid:,}", ephemeral=True)
                            return
                        if amt <= current_amt:
                            await modal_interaction.response.send_message(f"Bid must be higher than current highest ({current_amt:,})", ephemeral=True)
                            return
                        # record bid
                        bid_entry = {'user': modal_interaction.user.id, 'amount': amt, 'time': datetime.now(timezone.utc).timestamp()}
                        bids = auc.get('bids', [])
                        bids.append(bid_entry)
                        auc['bids'] = bids
                        auc['highest'] = (modal_interaction.user.id, amt)
                        self.bot.active_auctions[self.aid] = auc

                    # update embed in channel to reflect new highest bid
                    try:
                        update_auction_message(self.bot, auc)
                    except Exception:
                        pass

                    try:
                        await modal_interaction.response.send_message(f"Bid of {amt:,} registered.", ephemeral=True)
                    except Exception:
                        pass

            try:
                await interaction.response.send_modal(BidModal())
            except Exception:
                # fallback: ask for bid via ephemeral message
                await interaction.response.send_message("Unable to open modal — please type your bid reply in chat to me.", ephemeral=True)

        bid_btn.callback = bid_cb
        self.add_item(bid_btn)


async def handle_auction_end(aid: str, delay: int):
    await asyncio.sleep(delay)
    async with DATA_LOCK:
        auc = bot.active_auctions.pop(aid, None)
    if not auc:
        return
    channel = bot.get_channel(auc.get('channel_id'))
    if not channel:
        return
    bids = auc.get('bids', [])
    highest = auc.get('highest')
    embed = discord.Embed(title="Auction Ended", description=f"Item: {auc.get('prize')}")
    embed.add_field(name="Highest", value=f"{highest[1]:,}" if highest else "No bids")
    embed.add_field(name="Host", value=f"<@{auc.get('creator')}>", inline=False)
    try:
        msg = None
        if auc.get('message_id'):
            try:
                msg = await channel.fetch_message(auc.get('message_id'))
            except Exception:
                msg = None
        if msg:
            try:
                await msg.edit(embed=embed, view=None)
            except Exception:
                await channel.send(embed=embed)
        else:
            await channel.send(embed=embed)
    except Exception:
        pass

    if highest:
        winner_id, amt = highest
        try:
            await channel.send(f"🏆 Auction ended — Congratulations <@{winner_id}>! You won {auc.get('prize')} with a bid of {amt:,}. Check your DMs.")
        except Exception:
            pass
        await award_credits_for_prize(bot, [winner_id], auc.get('prize'))
        # DM winner
        try:
            user = bot.get_user(winner_id)
            if user:
                await user.send(f"You won the auction for {auc.get('prize')} with a bid of {amt:,}. Please make a support ticket to arrange transfer.")
        except Exception:
            pass
    else:
        try:
            await channel.send("Auction ended with no valid bids.")
        except Exception:
            pass


@bot.tree.command(name="auction")
async def auction(interaction: discord.Interaction):
    """Create an auction (admin only)."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return

    class AuctionModal(discord.ui.Modal, title="Create Auction"):
        duration = discord.ui.TextInput(label="Duration (e.g. 5m, 1h)", style=discord.TextStyle.short, required=True)
        prize = discord.ui.TextInput(label="Prize description", style=discord.TextStyle.long, required=True)
        min_bid = discord.ui.TextInput(label="Minimum bid (e.g. 1k)", style=discord.TextStyle.short, required=True)

        async def on_submit(self, modal_interaction: discord.Interaction):
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "an item"
            try:
                min_amt = parse_bid_amount(self.min_bid.value.strip())
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid minimum bid: {e}", ephemeral=True)
                return
            aid = uuid.uuid4().hex[:8]
            end_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)
            auc = {
                'id': aid,
                'creator': modal_interaction.user.id,
                'end_time': end_time.timestamp(),
                'prize': prize_text,
                'creator': modal_interaction.user.id,
                'min_bid': min_amt,
                'bids': [],
                'highest': None,
                'channel_id': modal_interaction.channel.id if modal_interaction.channel else None,
                'message_id': None,
                'mode': 'Auction',
            }
            view = PlaceBidView(aid, bot)
            embed = discord.Embed(title=prize_text, description="Auction")
            ts = int(end_time.timestamp())
            embed.add_field(name="Minimum Bid", value=f"{min_amt:,}")
            embed.add_field(name="Host", value=f"<@{modal_interaction.user.id}>", inline=False)
            embed.add_field(name="Highest", value="No bids")
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            if auc['channel_id'] is None:
                await modal_interaction.response.send_message("Cannot determine channel to post auction.", ephemeral=True)
                return
            channel = bot.get_channel(auc['channel_id'])
            if channel is None:
                await modal_interaction.response.send_message("Channel not found.", ephemeral=True)
                return
            msg = await channel.send(embed=embed, view=view)
            auc['message_id'] = msg.id
            bot.active_auctions[aid] = auc
            asyncio.create_task(handle_auction_end(aid, seconds))
            await modal_interaction.response.send_message(f"Auction created (ends <t:{ts}:F> / <t:{ts}:R>).", ephemeral=True)

    try:
        await interaction.response.send_modal(AuctionModal())
    except Exception:
        logging.exception("Failed to open Auction modal")
        try:
            await interaction.response.send_message("Failed to open auction modal — please check bot permissions or try again.", ephemeral=True)
        except Exception:
            pass

if __name__ == '__main__':
    token = os.environ.get('DISCORD_TOKEN')
    if not token:
        print('Set DISCORD_TOKEN environment variable')
    else:
        bot.run(token)
