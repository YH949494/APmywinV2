from telegram import Update
from telegram.ext import Updater, MessageHandler, Filters, CallbackContext

# In-memory leaderboard (replace with DB in production)
leaderboard = {}

def handle_message(update: Update, context: CallbackContext):
    message = update.message
    text = message.text or ""
    has_photo = bool(message.photo)

    # Check for valid submission
    if "#mywin" in text and has_photo:
        parts = text.split("#mywin", 1)
        game_name = parts[1].strip() if len(parts) > 1 else ""
        if game_name:
            # Add +20 XP silently
            add_to_leaderboard(message.from_user.id, game_name, 20)
        else:
            message.delete()  # #mywin without game name
    else:
        message.delete()  # Invalid post

def add_to_leaderboard(user_id, game_name, xp):
    if user_id not in leaderboard:
        leaderboard[user_id] = {"xp": 0, "games": {}}
    leaderboard[user_id]["xp"] += xp
    if game_name not in leaderboard[user_id]["games"]:
        leaderboard[user_id]["games"][game_name] = 0
    leaderboard[user_id]["games"][game_name] += xp

def handle_reaction(update: Update, context: CallbackContext):
    message = update.message
    if message.reply_to_message:  # If this is a reaction
        original_user_id = message.reply_to_message.from_user.id
        add_to_leaderboard(original_user_id, "reaction", 2)  # +2 XP per reaction

updater = Updater("YOUR_BOT_TOKEN")
updater.dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
# Uncomment if reactions are supported
# updater.dispatcher.add_handler(MessageHandler(Filters.reaction, handle_reaction))

updater.start_polling()
updater.idle()
