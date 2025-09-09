import os
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, ContextTypes, filters

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set. Use fly secrets set BOT_TOKEN=...")

leaderboard = {}

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    text = message.text or ""
    has_photo = bool(message.photo)

    if "#mywin" in text and has_photo:
        parts = text.split("#mywin", 1)
        game_name = parts[1].strip() if len(parts) > 1 else ""
        if game_name:
            add_to_leaderboard(message.from_user.id, game_name, 20)
        else:
            await message.delete()
    else:
        await message.delete()

def add_to_leaderboard(user_id, game_name, xp):
    if user_id not in leaderboard:
        leaderboard[user_id] = {"xp": 0, "games": {}}
    leaderboard[user_id]["xp"] += xp
    leaderboard[user_id]["games"].setdefault(game_name, 0)
    leaderboard[user_id]["games"][game_name] += xp

app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

app.run_polling(poll_interval=5)
