import asyncio
import os
import random
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import logging

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


class JoinView(discord.ui.View):
    def __init__(self, gid: str, bot_ref):
        super().__init__(timeout=None)
        self.gid = gid
        self.bot = bot_ref
        join_btn = discord.ui.Button(label="Join", style=discord.ButtonStyle.success)

        async def join_cb(interaction: discord.Interaction):
            async with DATA_LOCK:
                game = None
                for d in (self.bot.active_giveaways, self.bot.active_sos, self.bot.active_dbd, self.bot.active_rps, self.bot.active_memory, self.bot.active_maze, self.bot.active_reactroulette):
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
                    new = discord.Embed(title=old.title, description=old.description, color=old.color)
                    # get the latest entries count from current game state to avoid stale counts
                    try:
                        latest_count = None
                        async with DATA_LOCK:
                            latest_game = None
                            for d in (self.bot.active_giveaways, self.bot.active_sos, self.bot.active_dbd, self.bot.active_rps, self.bot.active_memory, self.bot.active_maze, self.bot.active_reactroulette):
                                g = d.get(self.gid)
                                if g:
                                    latest_game = g
                                    break
                            if latest_game:
                                latest_count = len(latest_game.get('entries', []))
                    except Exception:
                        latest_count = None
                    for f in old.fields:
                        if f.name.lower() == 'entries':
                            new.add_field(name=f.name, value=str(latest_count if latest_count is not None else len(entries)))
                        else:
                            new.add_field(name=f.name, value=f.value)
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
        self.active_auctions = {}
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

        async def on_submit(self, modal_interaction: discord.Interaction):
            await modal_interaction.response.defer(thinking=True)
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
            }
            view = JoinView(gid, bot)
            embed = discord.Embed(title="Giveaway", description=f"Prize: {prize_text}", color=discord.Color.blurple())
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            embed.add_field(name="Winners", value=str(gw['winners']))
            embed.add_field(name="Mode", value="Giveaway")
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
    embed = discord.Embed(title="Giveaway Ended", description=f"Prize: {gw['prize']}")
    embed.add_field(name="Winners", value=', '.join(f"<@{w}>" for w in winners) if winners else "No valid entries")
    embed.add_field(name="Mode", value=str(gw.get('mode', 'Giveaway')))
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

        async def on_submit(self, modal_interaction: discord.Interaction):
            await modal_interaction.response.defer(thinking=True)
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a split-or-steal game"
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
            }
            view = JoinView(sid, bot)
            embed = discord.Embed(title="Split Or Steal: Join", description=f"Prize: {prize_text}")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            embed.add_field(name="Winners", value="2")
            embed.add_field(name="Mode", value="Split or Steal")
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

        async def on_submit(self, modal_interaction: discord.Interaction):
            await modal_interaction.response.defer(thinking=True)
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
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
            }
            view = JoinView(sid, bot)
            embed = discord.Embed(title="Rock Paper Scissors: Join", description=f"Prize: {prize_text}")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            embed.add_field(name="Winners", value="2")
            embed.add_field(name="Mode", value="Rock Paper Scissors")
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

        async def on_submit(self, modal_interaction: discord.Interaction):
            await modal_interaction.response.defer(thinking=True)
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
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
            }
            view = JoinView(sid, bot)
            embed = discord.Embed(title="Double Down: Join", description=f"Prize: {prize_text}")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            embed.add_field(name="Winners", value="1")
            embed.add_field(name="Mode", value="Double Down")
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

        async def on_submit(self, modal_interaction: discord.Interaction):
            await modal_interaction.response.defer(thinking=True)
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
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
            }
            view = JoinView(sid, bot)
            embed = discord.Embed(title="Memory: Join", description=f"Prize: {prize_text}")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            embed.add_field(name="Winners", value="1")
            embed.add_field(name="Mode", value="Memory")
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

        async def on_submit(self, modal_interaction: discord.Interaction):
            await modal_interaction.response.defer(thinking=True)
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
            }
            view = JoinView(sid, bot)
            embed = discord.Embed(title="Maze Runner: Join", description=f"Prize: {prize_text}")
            ts = int(end_time.timestamp())
            embed.add_field(name="Ends", value=f"<t:{ts}:F> (<t:{ts}:R>)")
            embed.add_field(name="Winners", value="1")
            embed.add_field(name="Mode", value="Maze Runner")
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


@bot.tree.command(name="sync")
@app_commands.describe(global_sync="If true, sync commands globally instead of to the guild")
async def sync(interaction: discord.Interaction, global_sync: bool = False):
    # admin-only
    if interaction.guild and not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("You must be a server administrator to run this.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    try:
        if global_sync:
            synced = await bot.tree.sync()
            logging.info("Manually synced %d global commands", len(synced))
            names = ', '.join(c.name for c in synced)
            await interaction.followup.send(f"Synced {len(synced)} global commands: {names}")
        else:
            gid = bot._guild_id or (interaction.guild.id if interaction.guild else None)
            if not gid:
                await interaction.followup.send("No guild id available to sync to.")
                return
            guild_obj = discord.Object(id=gid)
            synced = await bot.tree.sync(guild=guild_obj)
            logging.info("Manually synced %d commands to guild %s", len(synced), gid)
            names = ', '.join(c.name for c in synced)
            await interaction.followup.send(f"Synced {len(synced)} commands to guild {gid}: {names}")
    except Exception as e:
        logging.exception("Manual sync failed: %s", e)
        await interaction.followup.send(f"Sync failed: {e}")


@bot.tree.command(name="gwend")
@app_commands.describe(message_id="The message ID of the giveaway embed to end early")
async def gwend(interaction: discord.Interaction, message_id: str):
    """End a giveaway early by providing its message ID. Admins or the giveaway creator only."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    try:
        mid = int(message_id)
    except Exception:
        await interaction.followup.send("Invalid message id. Provide the numeric message id.", ephemeral=True)
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
            await interaction.followup.send("No active giveaway found with that message id.", ephemeral=True)
            return
        # permission check: admins only
        if not (interaction.guild and interaction.user.guild_permissions.administrator):
            await interaction.followup.send("You must be a server administrator to end giveaways.", ephemeral=True)
            return
        # remove giveaway to prevent scheduled handler from running
        gw = bot.active_giveaways.pop(found_gid, None)

    if not gw:
        await interaction.followup.send("Giveaway already ended or not found.", ephemeral=True)
        return

    entries = list(gw.get('entries', []))
    channel = bot.get_channel(gw.get('channel_id'))
    if not channel:
        await interaction.followup.send("Channel for this giveaway could not be found.", ephemeral=True)
        return

    try:
        message = await channel.fetch_message(gw.get('message_id'))
    except Exception:
        message = None

    winners = []
    if entries:
        k = min(gw.get('winners', 1), len(entries))
        winners = random.sample(entries, k)

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

    await interaction.followup.send("Giveaway ended early.", ephemeral=True)


@bot.tree.command(name="reroll")
@app_commands.describe(message_id="The message ID of the ended giveaway to reroll")
async def reroll(interaction: discord.Interaction, message_id: str):
    """Reroll a finished giveaway by message ID. Admins or the giveaway creator only."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    try:
        mid = int(message_id)
    except Exception:
        await interaction.followup.send("Invalid message id.", ephemeral=True)
        return

    snapshot = bot.recent_giveaways.get(mid)
    if not snapshot:
        await interaction.followup.send("No recent giveaway found with that message id.", ephemeral=True)
        return

    # permission check: admins only
    if not (interaction.guild and interaction.user.guild_permissions.administrator):
        await interaction.followup.send("You must be a server administrator to reroll giveaways.", ephemeral=True)
        return

    entries = list(snapshot.get('entries', []))
    prev_winners = list(snapshot.get('winners', []))
    k = int(snapshot.get('num_winners', 1))
    if not entries:
        await interaction.followup.send("No entries to choose from.", ephemeral=True)
        return

    # prefer to exclude previous winners if possible
    pool = [e for e in entries if e not in prev_winners]
    if len(pool) < k:
        pool = entries

    new_winners = random.sample(pool, min(k, len(pool)))

    # update snapshot winners
    snapshot['winners'] = new_winners

    # announce in channel
    channel = bot.get_channel(snapshot.get('channel_id'))
    emoji = random.choice(EMOJI_POOL)
    if not channel:
        await interaction.followup.send("Could not find the channel for this giveaway.", ephemeral=True)
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

    await interaction.followup.send("Reroll complete.", ephemeral=True)



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
    seq = mem.get('sequence', [])
    # wait a few seconds so the winner can read the announcement before the sequence starts
    await asyncio.sleep(3)
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
                    if correct:
                        try:
                            await channel.send(f"<@{winner}> submitted: {answer} — correct! WON {mem.get('prize')}!")
                        except Exception:
                            pass
                    else:
                        try:
                            await channel.send(f"<@{winner}> submitted: {answer} — incorrect for {mem.get('prize')}.")
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
    # choose winning emoji
    winning = random.choice(options)
    # If there are join entries, prefer them as candidates (giveaway-style).
    entries = list(rr.get('entries', []))
    candidates = []
    if entries:
        # choose up to 3 random joiners
        try:
            candidates = random.sample(entries, k=min(3, len(entries)))
        except Exception:
            candidates = entries[:3]
    else:
        # fallback: gather reactors for the winning emoji (preserve order as returned)
        reactors = []
        try:
            if msg:
                for react in msg.reactions:
                    try:
                        if str(react.emoji) == winning:
                            users = [u async for u in react.users()]
                            users = [u for u in users if u.id != bot.user.id]
                            reactors = [u.id for u in users]
                            break
                    except Exception:
                        continue
        except Exception:
            reactors = []

        # prefer recorded first reactor (if it exists and isn't already first)
        fr = rr.get('first_reactors', {})
        primary = fr.get(winning)
        if primary and primary in reactors:
            # move primary to front
            reactors = [primary] + [r for r in reactors if r != primary]
        elif primary and primary not in reactors:
            # if primary recorded but not in list, prepend
            reactors = [primary] + reactors

        # pick up to 3 candidates
        candidates = []
        seen = set()
        for r in reactors:
            if r in seen:
                continue
            seen.add(r)
            candidates.append(r)
            if len(candidates) >= 3:
                break

    emoji = random.choice(EMOJI_POOL)
    # sequentially DM up to 3 winners (from join entries) to choose the winning emoji in DM
    winner_announced = False
    for candidate in candidates:
        try:
            user = bot.get_user(candidate)
            if not user:
                continue
            # DM the candidate with the options and ask them to react with their choice
            try:
                opts_display = ' '.join(options)
                dm = await user.send(f"You are one of the selected contenders for Reaction Roulette for {rr.get('prize')}. React with your choice from: {opts_display}")
            except Exception:
                # cannot DM this candidate; skip to next
                continue

            # announce contender in channel and add option reactions in DM
            try:
                await channel.send(f"{random.choice(EMOJI_POOL)} Reaction Guess: Congrats <@{candidate}> — check your DMs! You have 5 minutes to choose the reaction.")
            except Exception:
                pass

            for e in options:
                try:
                    await dm.add_reaction(e)
                except Exception:
                    pass

            # wait for this user's reaction in DM (5 minutes)
            def check(reaction, usr):
                return usr.id == candidate and reaction.message.id == dm.id and str(reaction.emoji) in options

            try:
                reaction, usr = await bot.wait_for('reaction_add', check=check, timeout=300)
            except asyncio.TimeoutError:
                try:
                    await user.send("Time expired for your choice — moving to next contender.")
                except Exception:
                    pass
                continue

            chosen = str(reaction.emoji)
            # send a DM acknowledgement that their answer was submitted
            try:
                await usr.send("Your answer has been submitted. Good luck!")
            except Exception:
                pass

            if chosen == winning:
                # announce winner in channel
                try:
                    await channel.send(f"{emoji} Reaction Guess: {winning} was chosen — <@{candidate}> selected it and wins {rr.get('prize')}! {emoji}")
                except Exception:
                    pass
                winner_announced = True
                break
            else:
                try:
                    await user.send(f"You selected {chosen} which is not the winning option. Moving to next contender.")
                except Exception:
                    pass
                continue
        except Exception:
            continue

    if not winner_announced:
        try:
            await channel.send(f"{emoji} Reaction Roulette result: {winning} was chosen but no contender selected it — no winner.")
        except Exception:
            pass


@bot.tree.command(name="reactroulette")
async def reactroulette(interaction: discord.Interaction):
    """Create a reaction roulette: bot posts a set of emoji, users react, bot picks a winning emoji and earliest reactor wins."""
    class RRModal(discord.ui.Modal, title="Create Reaction Roulette"):
        duration = discord.ui.TextInput(label="Duration (e.g. 30s, 1m)", style=discord.TextStyle.short, placeholder="30s", required=True)
        prize = discord.ui.TextInput(label="Prize description", style=discord.TextStyle.long, placeholder="Describe the prize", required=True)
        options = discord.ui.TextInput(label="Options (comma-separated emoji count)", style=discord.TextStyle.short, placeholder="3", required=False)

        async def on_submit(self, modal_interaction: discord.Interaction):
            await modal_interaction.response.defer(thinking=True)
            try:
                seconds = parse_duration(self.duration.value)
            except Exception as e:
                await modal_interaction.response.send_message(f"Invalid duration: {e}", ephemeral=True)
                return
            prize_text = self.prize.value.strip() or "a prize"
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
            }
            if rr['channel_id'] is None:
                await modal_interaction.response.send_message("Cannot determine channel to post.", ephemeral=True)
                return
            channel = bot.get_channel(rr['channel_id'])
            if channel is None:
                await modal_interaction.response.send_message("Channel not found.", ephemeral=True)
                return
            opts_display = ' '.join(opts)
            embed = discord.Embed(title="Reaction Roulette", description=f"React with one of the options to try to win: {opts_display}")
            embed.add_field(name="Prize", value=prize_text)
            embed.add_field(name="Host", value=f"<@{modal_interaction.user.id}>", inline=False)
            embed.add_field(name="Ends", value=f"<t:{int(rr['end_time'])}:R>")
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
    elif c0 == 'steal' and c1 == 'steal':
        verdict = "Both players chose steal — no one gets anything."
    else:
        if c0 == 'steal' or c1 == 'steal':
            stealer = winners[0] if c0 == 'steal' else winners[1]
            verdict = f"Giveaway result: <@{stealer}> wins — they get {prize}!"
            # special flourish when one split and one steal
            quote = '"The ultimate betrayal." — what a backstab.'

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
            await modal_interaction.response.defer(thinking=True)
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
            embed = discord.Embed(title="Auction", description=f"Item: {prize_text}")
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
