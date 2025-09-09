import os
from telegram import Update, Message
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    ContextTypes,
    filters,
    ReactionHandler,
)
from pymongo import MongoClient

# 🔑 Load secrets
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set. Use: fly secrets set BOT_TOKEN=...")
if not MONGO_URL:
    raise ValueError("MONGO_URL is not set. Use: fly secrets set MONGO_URL=...")

# 📦 MongoDB setup (same DB as AP bot)
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]
mywin_posts = db["mywin_posts"]  # store valid posts
reactions_collection = db["mywin_reactions"]  # track user reactions


# 🎮 Add XP to MongoDB
def add_xp(user_id, xp, game_name=None):
    update = {"$inc": {"xp": xp, "weekly_xp": xp, "monthly_xp": xp}}
    if game_name:
        update["$inc"][f"games.{game_name}"] = xp

    users_collection.update_one(
        {"_id": user_id},
        update,
        upsert=True,
    )


# 📩 Handle #mywin posts
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
            add_xp(message.from_user.id, 20, game_name)

            # Store message_id so reactions can be tracked later
            mywin_posts.update_one(
                {"_id": message.message_id},
                {
                    "$set": {
                        "chat_id": message.chat_id,
                        "user_id": message.from_user.id,
                        "game": game_name,
                    }
                },
                upsert=True,
            )

            await message.reply_text(f"✅ +20 XP for {game_name}!")
        else:
            await message.delete()
    else:
        await message.delete()


# ⭐ Handle reactions (+2 XP only once per user per post)
async def handle_reaction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reaction = update.reaction
    if not reaction or not reaction.new_reaction:  # skip removals
        return

    original: Message = reaction.message
    if not original:
        return

    # Check if this is a valid #mywin post
    if not mywin_posts.find_one({"_id": original.message_id}):
        return

    user_id = reaction.user.id
    message_id = original.message_id

    # Check if this user already reacted to this post
    if reactions_collection.find_one({"user_id": user_id, "message_id": message_id}):
        return  # already gave XP

    # Award XP and log reaction
    add_xp(user_id, 2)
    reactions_collection.insert_one({"user_id": user_id, "message_id": message_id})


# 🚀 Run bot
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Message handler for #mywin posts
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Reaction handler
    app.add_handler(ReactionHandler(handle_reaction))

    app.run_polling(poll_interval=5)


if __name__ == "__main__":
    main()
