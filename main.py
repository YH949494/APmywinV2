import os
from telegram import Update
from telegram.ext import Updater, MessageHandler, CallbackContext, filters

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set. Use fly secrets set BOT_TOKEN=...")

leaderboard = {}

def handle_message(update: Update, context: CallbackContext):
    message = update.message
    text = message.text or ""
    has_photo = bool(message.photo)

    if "#mywin" in text and has_photo:
        parts = text.split("#mywin", 1)
        game_name = parts[1].strip() if len(parts) > 1 else ""
        if game_name:
            add_to_leaderboard(message.from_user.id, game_name, 20)
        else:
            message.delete()
    else:
        message.delete()

def add_to_leaderboard(user_id, game_name, xp):
    if user_id not in leaderboard:
        leaderboard[user_id] = {"xp": 0, "games": {}}
    leaderboard[user_id]["xp"] += xp
    if game_name not in leaderboard[user_id]["games"]:
        leaderboard[user_id]["games"][game_name] = 0
    leaderboard[user_id]["games"][game_name] += xp

updater = Updater(BOT_TOKEN)
updater.dispatcher.add_handler(
    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
)

# Poll every 5 seconds
updater.start_polling(poll_interval=5)
updater.idle()
