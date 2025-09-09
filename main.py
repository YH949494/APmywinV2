import os
from pymongo import MongoClient
from telegram import Update, Message
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    ContextTypes, 
    TypeHandler,
    filters,
)

# --- Secrets --- 
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set. Use: fly secrets set BOT_TOKEN=...")
if not MONGO_URL:
    raise ValueError("MONGO_URL is not set. Use: fly secrets set MONGO_URL=...")

# --- Mongo ---
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]
mywin_posts = db["mywin_posts"]          # tracks valid #mywin messages
reactions_collection = db["mywin_reacts"] # tracks who reacted to which post

def add_xp(user_id: int, xp: int, game_name: str | None = None):
    update = {"$inc": {"xp": xp, "weekly_xp": xp, "monthly_xp": xp}}
    if game_name:
        update["$inc"][f"games.{game_name}"] = xp
    users_collection.update_one({"_id": user_id}, update, upsert=True)

# --- #mywin handler ---
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    caption = message.caption or ""

    # Check: must have photo AND valid #mywin caption
    if message.photo and caption.startswith("#mywin"):
        parts = caption.split("#mywin", 1)
        game_name = parts[1].strip() if len(parts) > 1 else ""
        if game_name:  # valid #mywin <game>
            add_xp(message.from_user.id, 20, game_name)
            await message.reply_text(f"✅ +20 XP for {game_name}!")
            return

    # Delete any message that does not match the rules
    await message.delete()

# --- Reaction handler (works without importing ReactionUpdated) ---
async def handle_any_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # We’ll only process if this update is a reaction change
    mr = getattr(update, "message_reaction", None)
    if not mr:
        return

    original: Message = mr.message
    if not original:
        return

    # Only count reactions on tracked #mywin posts
    if not mywin_posts.find_one({"_id": original.message_id}):
        return

    user_id = mr.user.id
    message_id = original.message_id

    # Give +2 XP only once per user per post
    if reactions_collection.find_one({"user_id": user_id, "message_id": message_id}):
        return

    add_xp(user_id, 2)
    reactions_collection.insert_one({"user_id": user_id, "message_id": message_id})

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # text posts
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # catch-all updates, we filter for reactions inside
    app.add_handler(TypeHandler(Update, handle_any_update))

    app.run_polling(poll_interval=5)

if __name__ == "__main__":
    main()
